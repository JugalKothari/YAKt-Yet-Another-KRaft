import threading
from pysyncobj import SyncObj, replicated
import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from argparse import ArgumentParser
from pysyncobj.utility import TcpUtility, UtilityException
import requests

# Example SyncObj class
class MySyncObj(SyncObj):
    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(MySyncObj, self).__init__(selfNodeAddr, otherNodeAddrs)
        self.value = 0

    def onStateChanged(self, newState):
        print(f"Node {self._selfNodeAddr} changed state to {newState}")

    @replicated
    def set_value(self, value):
        self.value = value

    @replicated
    def get_value(self):
        return self.value

def checkCorrectAddress(address):
    try:
        host, port = address.rsplit(':', 1)
        port = int(port)
        assert (port > 0 and port < 65536)
        print('address is right')
        return True
    except:
        return False

def executeAdminCommand(args):
    parser = ArgumentParser()
    parser.add_argument('-conn', action='store', dest='connection', help='address to connect')
    parser.add_argument('-status', action='store_true', help='send command \'status\'')
    parser.add_argument('-add', action='store', dest='add', help='send command \'add\'')
    parser.add_argument('-remove', action='store', dest='remove', help='send command \'remove\'')
    parser.add_argument('-set_version', action='store', dest='version', type=int, help='set cluster code version')

    data = parser.parse_args(args)
    if not checkCorrectAddress(data.connection):
        return 'invalid address to connect'

    if data.status:
        message = ['status']
    elif data.add:
        if not checkCorrectAddress(data.add):
            return 'invalid address to command add'
        message = ['add', data.add]
    elif data.remove:
        if not checkCorrectAddress(data.remove):
            return 'invalid address to command remove'
        message = ['remove', data.remove]
    elif data.version is not None:
        message = ['set_version', data.version]
    else:
        return 'invalid command'

    util = TcpUtility('')
    try:
        result = util.executeCommand(data.connection, message)
    except UtilityException as e:
        return str(e)

    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        return '\n'.join('%s: %s' % (k, v) for k, v in sorted(result.items()))
    return str(result)

def get_syncobj_status(node_address):
    try:
        print(f"Sending request to {node_address} with command -status")

        # Use requests to make the GET request
        response = requests.get(f"http://{node_address}/status")
        
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            return f"Request failed with status code: {response.status_code}"

    except Exception as e:
        return f"Request failed: {str(e)}"

class MyRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, syncObj, **kwargs):
        self.syncObj = syncObj
        super().__init__(*args, **kwargs)

    def do_GET(self):
        try:
            print(f"Received GET request for {self.path}")

            if self.path == '/status':
                # Extracting the port number from the request's URL
                node_address = f"localhost:{self.server.server_port}"
                status = get_syncobj_status(node_address)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')  # Adjust as needed
                self.end_headers()
                self.wfile.write(json.dumps(status).encode())
            elif self.path == '/get':
                value = self.syncObj.get_value()  # Use the same syncObj for all nodes
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(str(value).encode())
            else:
                self.send_response(404)
                self.end_headers()

        except Exception as e:
            print(f"Error processing GET request: {e}")
            self.send_response(500)
            self.end_headers()

    def do_POST(self):
        try:
            if self.path == '/set':
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                value = int(post_data.decode())
                self.syncObj.set_value(value)  # Use the same syncObj for all nodes
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'OK')
            else:
                self.send_response(404)
                self.end_headers()

        except Exception as e:
            print(f"Error processing POST request: {e}")
            self.send_response(500)
            self.end_headers()

def start_http_server(port, syncObj):
    server_address = ('', port)
    httpd = ThreadingHTTPServer(server_address, MyRequestHandler)
    httpd.syncObj = syncObj
    print(f'Starting HTTP server on port {port}')
    httpd.serve_forever()

if __name__ == '__main__':
    # Start the SyncObj node
    syncObj1 = MySyncObj('localhost:1000', ['localhost:1001', 'localhost:1002'])
    syncObj2 = MySyncObj('localhost:1001', ['localhost:1000', 'localhost:1002'])
    syncObj3 = MySyncObj('localhost:1002', ['localhost:1001', 'localhost:1000'])

    # Start HTTP servers in separate threads
    http_thread1 = threading.Thread(target=start_http_server, args=(1000, syncObj1))
    http_thread2 = threading.Thread(target=start_http_server, args=(1001, syncObj2))
    http_thread3 = threading.Thread(target=start_http_server, args=(1002, syncObj3))
    http_thread1.start()
    http_thread2.start()
    http_thread3.start()

    # Wait for all threads to finish
    http_thread1.join()
    http_thread2.join()
    http_thread3.join()

#!/usr/bin/env python
from __future__ import print_function
import os
from rich.console import Console
import datetime
import threading
from flask import Flask, jsonify, request
import sys
import time
from node.raftnode import RaftNode
import requests
import json
# sys.path.append("../")
console = Console()
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)

def serialize_datetime_recursive(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_datetime_recursive(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_datetime_recursive(item) for item in obj]
    else:
        return obj
def onAdd(res, err, cnt):
    print('onAdd %d:' % cnt, res, err)


app = Flask(__name__)

flask_running = True


def signal_handler(sig, frame):
    global flask_running
    print('Received signal to terminate Flask server.')
    flask_running = False


@app.route('/', methods=['GET'])
def index():
    print(o._getLeader())
    port = int(sys.argv[1])
    print((o._getLeader().port))
    if int(o._getLeader().port) == port:
        return jsonify({'Hey There!': "Welcome to the kRaft!"})    
    return jsonify({'Hey There!': "Welcome to the kRaft Implementation!"})

@app.route('/counter', methods=['GET'])
def get_counter():
    return jsonify({'counter': o.getCounter()})


@app.route('/metadata-fetch-broker', methods=['POST'])
def get_registered_brokers():
    try:
        fetch_data_details = request.json
        brokerId = fetch_data_details['brokerId']
        offset = fetch_data_details['offset']
        res = o.metadataFetch(brokerId=brokerId, offset=offset)
        return jsonify({'Latest Metadata': res})
    except Exception as e:
        return jsonify({'Some Error Occured': f"{e}"})
    return

@app.route('/metadata-fetch-client', methods = ['GET'])
def get_all_metadata():
    try:
        # offset_param = request.args.get('offset', default=None)
        # if offset_param is not None:
        #     last_aware_date_of_broker = datetime.datetime.fromtimestamp(int(offset_param))
        #     console.log(last_aware_date_of_broker)
        res = o.metadataFetchClient(offset='NA')
        return jsonify({'Latest Metadata': f"{res}"})
    except Exception as e:
        return jsonify({'An Unexpected Error Occured': f"{e}"})


@app.route('/register-broker-record', methods=['POST'])
def register_broker_record():
    try:
        register_data = request.json
        fields = register_data['fields']
        
        port = int(sys.argv[1])
        current_leader_port = int(o._getLeader().port)

        if current_leader_port == port:
            console.log('[blue bold]LEADER GOT REQUEST')
            o.registerNewBroker(broker_information=fields, last_updated_time_stamp=datetime.datetime.now())
            return jsonify({'message': 'Broker registered successfully'})
        else:
            console.log('[blue bold]NOT LEADER. FORWARDING REQUEST TO LEADER.')
            leader_url = f'http://localhost:{node_port_mapping[current_leader_port]}/register-broker-record'
            response = requests.post(leader_url, json=register_data)
            console.log(f'[blue bold]Response from Leader: {response.text}')
            return jsonify({'message': f'Request Sent to Leader, Broker registered successfully By Leader {response.text}'})
    except Exception as e:
        return jsonify({'message': f'Error during broker registration: {str(e)}'})


@app.route('/register-broker-change', methods=['POST'])
def register_broker_change():
    try:
        register_data = request.json
        fields = register_data['fields']
        
        current_leader_port = int(o._getLeader().port)

        if current_leader_port == int(sys.argv[1]):
            console.log('[blue bold]LEADER GOT REQUEST FOR BROKER CHANGE')
            o.registerBrokerChange(broker_information=fields, last_updated_time_stamp=datetime.datetime.now())
            return jsonify({'message': 'Broker Update Done successfully'})
        else:
            console.log('[blue bold]NOT LEADER. FORWARDING REQUEST TO LEADER FOR BROKER CHANGE.')
            leader_url = f'http://localhost:{node_port_mapping[current_leader_port]}/register-broker-change'
            response = requests.post(leader_url, json=register_data)
            console.log(f'[blue bold]Response from Leader for Broker Change: {response.text}')
            return jsonify({'message': f'Request Sent to Leader, Broker Update Done successfully By Leader {response.text}'})
    except Exception as e:
        return jsonify({'message': f'Error during broker update: {str(e)}'})

@app.route('/register-topic-record', methods=['POST'])
def register_topic():
    try:
        register_data = request.json
        fields = register_data['fields']
        timestamp = datetime.datetime.now()

        current_leader_port = int(o._getLeader().port)
        if current_leader_port == int(sys.argv[1]):
            console.log('[blue bold]LEADER GOT REQUEST FOR TOPIC REGISTRATION')
            res = o.registerTopic(topic_information=fields, timestamp=timestamp)
            return jsonify({'message': f"Success ! Topic Registered ! "})
        else:
            console.log('[blue bold]NOT LEADER. FORWARDING REQUEST TO LEADER FOR TOPIC REGISTRATION.')
            leader_url = f'http://localhost:{node_port_mapping[current_leader_port]}/register-topic-record'
            response = requests.post(leader_url, json=register_data)
            console.log(f'[blue bold]Response from Leader for Topic Registration: {response.text}')
            return jsonify({'message': f'Request Sent to Leader, Topic Registered successfully By Leader {response.text}'})
    except Exception as e:
        return jsonify({'message': f'Error during topic registration: {str(e)}'})

@app.route('/delete-broker-record', methods = ['POST'])
def delete_broker():
    try:
        register_data = request.json
        brokerId = register_data['brokerId']
        timestamp = datetime.datetime.now()
        return_uuid = o.deleteBroker(brokerId=brokerId, timestamp = timestamp)
        return jsonify({'message': f'Deletion Operation Complete  !'})
    except Exception as e:
        return jsonify({'message': f'Error during topic registration: {str(e)}'})

@app.route('/register-producer-record', methods=['POST'])
def producer_record():
    try:
        register_data = request.json
        timestamp = datetime.datetime.now()
        current_leader_port = int(o._getLeader().port)
        if current_leader_port == int(sys.argv[1]):
            console.log('[blue bold]LEADER GOT REQUEST FOR PRODUCER REGISTRATION')
            res = o.producerIdRecord(timestamp=timestamp, producer_information=register_data)
            return jsonify({'message': f"Success! Producer Record Registered! {res}"})
        else:
            console.log('[blue bold]NOT LEADER. FORWARDING REQUEST TO LEADER FOR PRODUCER REGISTRATION.')
            leader_url = f'http://localhost:{node_port_mapping[current_leader_port]}/register-producer-record'
            response = requests.post(leader_url, json=register_data)
            console.log(f'[blue bold]Response from Leader for Producer Registration: {response.text}')
            return jsonify({'message': f'Request Sent to Leader, Producer Record Registered successfully By Leader {response.text}'})
    except Exception as e:
        return jsonify({'message': f'Error during producer record registration: {str(e)}'})

@app.route('/partition-record', methods=['POST'])
def partition_record():
    try:
        partition_data = request.json
        timestamp = datetime.datetime.now()
        current_leader_port = int(o._getLeader().port)
        if current_leader_port == int(sys.argv[1]):
            console.log('[blue bold]LEADER GOT REQUEST FOR PARTITION RECORD')
            lead=str(o._getLeader())
            res = o.partitionRecord(timestamp=timestamp, partition_information=partition_data,leader=lead)
            return jsonify({'message': f"Success! Partition Record Registered! {res}"})
        else:
            console.log('[blue bold]NOT LEADER. FORWARDING REQUEST TO LEADER FOR PARTITION RECORD.')
            leader_url = f'http://localhost:{node_port_mapping[current_leader_port]}/partition-record'
            response = requests.post(leader_url, json=partition_data)
            console.log(f'[blue bold]Response from Leader for Partition Record: {response.text}')
            return jsonify({'message': f'Request Sent to Leader, Partition Record Registered successfully By Leader {response.text}'})
    except Exception as e:
        return jsonify({'message': f'Error during partition record registration: {str(e)}'})



def flask_thread(port):
    app.run(port=port)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' %
              sys.argv[0])
        sys.exit(-1)
    node_port_mapping = {
        1000: 4000,
        1001: 4001,
        1002: 4002,
        1003: 4003,
        1004: 4004
    }
    port = int(sys.argv[1])
    partners = ['localhost:%d' % int(p) for p in sys.argv[2:]]
    o = RaftNode('localhost:%d' % port, partners)

    raft_flask_thread = threading.Thread(
        target=flask_thread, args=(node_port_mapping[port],), daemon=True)
    raft_flask_thread.daemon = True
    raft_flask_thread.start()

    # leader_election_thread = threading.Thread(target=leader_election_thread, daemon=True)
    # leader_election_thread.start()
    # Set up a signal handler to terminate the Flask server on Ctrl+C
    # signal.signal(signal.SIGINT, signal_handler)
    # try:
    # raft_flask_thread.start()
    # except (KeyboardInterrupt, SystemExit):
    # raft_flask_thread.st

    n = 0
    old_value = -1
    step = 0
    lastIndex = -1
    log_folder = 'logs'
    os.makedirs(log_folder, exist_ok=True)
    while True:
        # time.sleep(0.005)
        time.sleep(0.1)
        step += 1
        if o._getLeader() is None:
            continue
        raftIndex = o.raftCommitIndex
        if raftIndex != lastIndex:
            console.log(f"[magenta bold] Raft Commit Index Changed! >> {o.raftCommitIndex}")
        lastIndex = o.raftCommitIndex
        if step % 10 == 0:
            console.log(f' Leader is {o._getLeader()}')

        if step % 50 == 0:
            if int(o._getLeader().port) == int(sys.argv[1]):
                file_number = step // 50 - 1
                file_path = os.path.join(log_folder, f'{file_number}.json')
                with open(file_path, 'w') as json_file:
                    serialized_logs = serialize_datetime_recursive(o.logs)
                    json.dump(serialized_logs, json_file, cls=DateTimeEncoder, indent=4)
                    console.log(f'Logs written to JSON file: {file_path}')
        # if step % 50 == 0:
            # console.log(o.logs)
        # print('Counter value:', o._getLeader(), o.raftCommitIndex, o.logs)

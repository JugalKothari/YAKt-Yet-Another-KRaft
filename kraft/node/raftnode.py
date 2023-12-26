from pysyncobj import SyncObj, replicated
import uuid
from rich.console import Console
import datetime
console = Console()


class RaftNode(SyncObj):
    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(RaftNode, self).__init__(selfNodeAddr, otherNodeAddrs)
        self.__counter = 0
        self.brokers = []
        self.logs = {
            "RegisterBrokerRecords": {
                "BrokerLogs": {},
                "GlobalLogs": [],  # Broker Specific Logs
                "BrokerInformation": {}
                # BrokerID: Broker Information
            },
            "TopicRecords": {
                "Topics": {},  # Topics present in the system.
                "TopicRecords": []  # Records for the topics
            },
            "PartitionRecords": {
                "Partitions": {},
                "PartitionRecords": []
            },
            "ProducerRecords": {
                "Producers": {},
                "ProducerRecords": []
            },
            "LastUpdatedTimeStamp": ""
        }
        # Main MetaData Store for all the RaftCluster.

    @replicated
    def registerNewBroker(self, broker_information, last_updated_time_stamp):
        if broker_information['brokerId'] not in self.brokers:
            broker_id = broker_information['brokerId']
            # Add the broker ID to the list
            self.brokers.append(broker_information['brokerId'])
            broker_info = {
                "internalUUID": str(uuid.uuid4()),
                "brokerId": broker_information.get("brokerId", 0),
                "brokerHost": broker_information.get("brokerHost", ""),
                "brokerPort": broker_information.get("brokerPort", ""),
                "securityProtocol": broker_information.get("securityProtocol", ""),
                "brokerStatus": broker_information.get("brokerStatus", "ALIVE"),
                "rackId": broker_information.get("rackId", ""),
                "epoch": broker_information.get("epoch", 0),
                "lastUpdatedTimeStamp": last_updated_time_stamp
            }
            # Add broker information to logs
            console.log(broker_info)
            self.logs["RegisterBrokerRecords"]["BrokerInformation"][str(
                broker_id)] = broker_info
            self.logs["LastUpdatedTimeStamp"] = last_updated_time_stamp
            self.logs["RegisterBrokerRecords"]["GlobalLogs"].append(
                broker_info)

            if str(broker_id) in self.logs["RegisterBrokerRecords"]["BrokerLogs"]:
                curLogs = self.logs["RegisterBrokerRecords"]["BrokerLogs"][str(
                    broker_id)]
                curLogs.append(broker_info)
                self.logs["RegisterBrokerRecords"]["BrokerLogs"][str(
                    broker_id)] = curLogs
            else:
                logs = []
                logs.append(broker_info)
                self.logs["RegisterBrokerRecords"]["BrokerLogs"][str(
                    broker_id)] = logs
            return 1
        else:
            console.log('[red] Nope, Broker Already Exists')
            return 0  # Returning 0 to indicate that the broker is already registered

    @replicated
    def registerBrokerChange(self, broker_information, last_updated_time_stamp):
        if broker_information['brokerId'] in self.brokers:
            brokerId = broker_information['brokerId']
            staleInfo = self.logs["RegisterBrokerRecords"]["BrokerInformation"][str(
                brokerId)]
            broker_info = {
                "internalUUID": staleInfo["internalUUID"],
                "brokerId": broker_information.get("brokerId", 0),
                "brokerHost": broker_information.get("brokerHost", "NA"),
                "brokerPort": broker_information.get("brokerPort", "NA"),
                "securityProtocol": broker_information.get("securityProtocol", "NA"),
                "brokerStatus": broker_information.get("brokerStatus", "ALIVE"),
                "rackId": staleInfo["rackId"],
                "epoch": staleInfo["epoch"] + 1,
                "lastUpdatedTimeStamp": last_updated_time_stamp
            }
            console.log(broker_info)
            self.logs["RegisterBrokerRecords"]["BrokerInformation"][str(
                brokerId)] = broker_info
            self.logs["LastUpdatedTimeStamp"] = last_updated_time_stamp
            self.logs["RegisterBrokerRecords"]["GlobalLogs"].append(
                broker_info)
            if str(brokerId) in self.logs["RegisterBrokerRecords"]["BrokerLogs"]:
                curLogs = self.logs["RegisterBrokerRecords"]["BrokerLogs"][str(
                    brokerId)]
                curLogs.append(broker_info)
                self.logs["RegisterBrokerRecords"]["BrokerLogs"][str(
                    brokerId)] = curLogs
            else:
                logs = []
                logs.append(broker_info)
                self.logs["RegisterBrokerRecords"]["BrokerLogs"][str(
                    brokerId)] = logs

            console.log(f"[green bold] Broker {brokerId} Updated!  ")
            return 1

        else:
            console.log('[red bold] Broker Does Not Exist!')
            return 0

    @replicated
    def deleteBroker(self, brokerId, timestamp):
        self.logs["LastUpdatedTimeStamp"] = timestamp
        try:
            if str(brokerId) in self.logs["RegisterBrokerRecords"]["BrokerLogs"]:
                console.log(self.logs["RegisterBrokerRecords"]["BrokerInformation"][f"{brokerId}"]["internalUUID"])
                del self.logs["RegisterBrokerRecords"]["BrokerLogs"][f"{brokerId}"]
                del self.logs["RegisterBrokerRecords"]["BrokerInformation"][f"{brokerId}"]
            else:
                console.log(f"[red bold] Broker Not Present !")
        except Exception as e:
            console.log(f"[red bold] {e}")

    @replicated
    def producerIdRecord(self, producer_information, timestamp):
        producerId = producer_information['fields']['producerId']
        brokerId = producer_information['fields']['brokerId']
        self.logs["LastUpdatedTimeStamp"] = timestamp
        if str(brokerId) in self.logs["RegisterBrokerRecords"]["BrokerLogs"]:
                curLogs = self.logs["RegisterBrokerRecords"]["BrokerLogs"][str(brokerId)]
                epoch = curLogs[-1]['epoch']
                context = {
                    "type": "metadata",
                    "name": "ProducerIdsRecord",
                    "fields": {
                        "brokerId": producer_information['fields']['brokerId'],
                        "brokerEpoch": epoch,
                        "producerId": producerId
                    },
                    "timestamp": timestamp
                }
                if producerId not in self.logs["ProducerRecords"]["Producers"]:
                    self.logs["ProducerRecords"]["Producers"][producerId] = 'PRODUCER'
                    self.logs['ProducerRecords']["ProducerRecords"].append(context)
                    return 1
        else:
            console.log('[red bold] Broker Does Not Exist From where request is coming !')
            return 0
        

    @replicated
    def partitionRecord(self, partition_information, timestamp,leader):
        partitionId = partition_information['fields']['partitionId']
        topicUUID = partition_information['fields']['topicUUID']
        replicas = partition_information['fields']['replicas']
        ISR = partition_information['fields']['ISR']
        removingReplicas = partition_information['fields']['removingReplicas']
        addingReplicas = partition_information['fields']['addingReplicas']
        leader = leader
        partitionEpoch = partition_information['fields']['partitionEpoch']

        # Your logic to update the state based on partition_information
        # ...

        # Example: Updating logs or state
        partition_context = {
            "type": "metadata",
            "name": "PartitionRecord",
            "fields": {
                "partitionId": partitionId,
                "topicUUID": topicUUID,
                "replicas": replicas,
                "ISR": ISR,
                "removingReplicas": removingReplicas,
                "addingReplicas": addingReplicas,
                "leader": leader,
                "partitionEpoch": partitionEpoch
            },
            "timestamp": timestamp
        }

        # Assuming self.logs is a dictionary that stores partition records
        if partitionId not in self.logs["PartitionRecords"]:
            self.logs["PartitionRecords"][partitionId] = []

        self.logs["PartitionRecords"][partitionId].append(partition_context)
        self.logs["LastUpdatedTimeStamp"] = timestamp

        # Return any relevant information or success code
        return 1  # Example success code

#         {
# 	"type": "metadata",
# 	"name": "ProducerIdsRecord",
# 	"fields": {
# 		"brokerId": "", // type : string; uuid of requesting broker; given by client
# 		"brokerEpoch": 0, // type : int; the epoch at which broker requested; set to broker epochl
# 		"producerId": 0 // type : int; producer id requested; given by client
# 	},
# 	"timestamp": "" // type: timestamp
# }
        pass

    def metadataFetch(self, brokerId, offset):
        lastAwareTime = datetime.datetime.strptime(
            offset, "%a, %d %b %Y %H:%M:%S %Z")
        # context = {}
        if datetime.datetime.now() - lastAwareTime > datetime.timedelta(minutes=10):
            # context['BrokerLogs'] = self.logs["RegisterBrokerRecords"]["GlobalLogs"]
            # context['TopicLogs'] = self.logs["TopicRecords"]["TopicRecords"]
            return self.logs["RegisterBrokerRecords"]["GlobalLogs"]
        latestInformation = [info for info in self.logs["RegisterBrokerRecords"]["GlobalLogs"]
                             if info['lastUpdatedTimeStamp'] > lastAwareTime and info['brokerId'] == brokerId]
        # context['BrokerLogs'] = latestInformation
        # context['TopicLogs'] = [info for info in self.logs["TopicRecords"]["TopicRecords"]
        #                         if info['timstamp'] > lastAwareTime]
        return latestInformation

    def metadataFetchClient(self, offset):
        try:
            context = {}
            context['BrokerLogs'] = self.logs["RegisterBrokerRecords"]["GlobalLogs"]
            context['TopicLogs'] = self.logs["TopicRecords"]["TopicRecords"]
            context['PartitionLogs'] = self.logs['PartitionRecords']['PartitionRecords']
            console.log(f"[magenta bold] metadata fetch call has been done!")
            return context
        except Exception as e:
            console.log(e)
            return context

    def getRegisteredBrokers(self):
        return self.brokers

    @replicated
    def registerPartition(self, partition_information, timestamp):
        partitionName = partition_information['fields']['partitionId']
        self.logs["LastUpdatedTimeStamp"] = timestamp
        if partitionName not in self.logs["PartitionRecords"]["Partitions"]:
            registerPartitionUUID = str(uuid.uuid4())
            self.logs["PartitionRecords"]["Partitions"][partitionName] = registerPartitionUUID
            self.logs['PartitionRecords']["PartitionRecords"].append(
                partition_information)
            return 1
        else:
            return 0

    @replicated
    def registerTopic(self, topic_information, timestamp):
        topicName = topic_information['name']
        self.logs["LastUpdatedTimeStamp"] = timestamp
        if topicName not in self.logs["TopicRecords"]["Topics"]:
            registerTopicUUID = str(uuid.uuid4())
            self.logs["TopicRecords"]["Topics"][topicName] = registerTopicUUID
            self.logs['TopicRecords']["TopicRecords"].append({
                "type": "metadata",
                "name": "TopicRecord",
                "fields": {
                    "topicUUID": registerTopicUUID,
                    "name": topicName
                },
                "timestamp": timestamp
            })
            return 1
        else:
            return 0

    @replicated
    def incCounter(self):
        self.__counter += 1
        return self.__counter

    @replicated
    def addValue(self, value, cn):
        self.__counter += value
        return self.__counter, cn

    def getCounter(self):
        return self.__counter

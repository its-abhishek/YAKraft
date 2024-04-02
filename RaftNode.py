import time
import threading
from pyraft import raft
from flask import Flask, Response, request
import requests
from flask_cors import CORS
import datetime
import json
import math

votes = 0
current_log_info = ''
current_log_id = 0
server_running = False
createBrokerVotes = 0
newBrokerRecord = ''
createTopicVotes = 0
newTopic = ''
createNewPartitionVotes = 0
NewPartition = ''
createNewProducerVotes = 0
NewProducer = ''
BrokerID = 0 
TopicID = 0
PartitionId = 0
updateBrokerRecordVotes = 0
newUpdateBrokerRecord = '' 

global app
app = Flask(__name__)
CORS(app)


def getPartitionID():
    global PartitionId
    return PartitionId
def incrementPartitionID():
    global PartitionId
    PartitionId += 1
    return PartitionId
def getBrokerID():
    global BrokerID
    return BrokerID
def incrementBrokerID():
    global BrokerID
    BrokerID += 1
    return BrokerID
def getTopicID():
    global TopicID
    return TopicID
def incrementTopicID():
    global TopicID
    TopicID +=1 
    return TopicID
def set_server_running():
    global server_running
    server_running = True
    return server_running
def get_server_running():
    global server_running
    return server_running
def update_log_id(id):
    global current_log_id 
    current_log_id = id
    return current_log_id
def increament_votes():
    global votes 
    votes +=1
    return votes
def set_votes():
    global votes 
    votes =1
    return votes
def get_votes():
    global votes
    return votes
def set_current_log(log):
    global current_log_info
    current_log_info = log
    return current_log_info
def get_current_log():
    global current_log_info
    return current_log_info
def get_current_log_id ():
    global current_log_id 
    return current_log_id
def increament_current_log_id():
    global current_log_id
    print(type(current_log_id))
    current_log_id = int(current_log_id)
    current_log_id += 1
    return current_log_id

def setCreateBrokerVotes():
    global createBrokerVotes
    createBrokerVotes = 1
    return createBrokerVotes
def incrementCreateBrokerVotes():
    global createBrokerVotes
    createBrokerVotes += 1
    return createBrokerVotes
def getCreateBrokerVotes():
    global createBrokerVotes
    return createBrokerVotes
def setNewBrokerRecord(record):
    global newBrokerRecord
    newBrokerRecord = record
    return newBrokerRecord
def getNewBrokerRecord():
    global newBrokerRecord
    return newBrokerRecord

def setCreateTopicVotes():
    global createTopicVotes
    createTopicVotes = 1
    return createTopicVotes
def incrementCreateTopicVotes():
    global createTopicVotes
    createTopicVotes += 1
    return createTopicVotes
def getCreateTopicVotes():
    global createTopicVotes
    return createTopicVotes
def setNewTopic(topic):
    global newTopic
    newTopic = topic
    return newTopic
def getNewTopic():
    global newTopic
    return newTopic

def setCreateNewPartionsVotes():
    global createNewPartitionVotes
    createNewPartitionVotes = 1
    return createNewPartitionVotes
def incrementeNewPartionsVotes():
    global createNewPartitionVotes
    createNewPartitionVotes += 1
    return createNewPartitionVotes
def geteNewPartionsVotes():
    global createNewPartitionVotes
    return createNewPartitionVotes
def setNewPartition(partition):
    global NewPartition
    NewPartition = partition
    return NewPartition
def getNewPartition():
    global NewPartition
    return NewPartition

def setCreateNewPartionsVotes():
    global createNewProducerVotes
    createNewProducerVotes = 1
    return createNewProducerVotes
def incrementeNewPartionsVotes():
    global createNewProducerVotes
    createNewProducerVotes += 1
    return createNewProducerVotes
def geteNewPartionsVotes():
    global createNewProducerVotes
    return createNewProducerVotes
def setNewProducer(Producer):
    global NewProducer
    NewProducer = Producer
    return NewProducer
def getNewProducer():
    global NewProducer
    return NewProducer

def setUpdateBrokerRecordVotes():
    global updateBrokerRecordVotes 
    updateBrokerRecordVotes = 1
    return updateBrokerRecordVotes

def getUpdateBrokerRecordVotes():
    global updateBrokerRecordVotes
    return updateBrokerRecordVotes

def incrementUpdateBrokerRecordVotes():
    global updateBrokerRecordVotes
    updateBrokerRecordVotes += 1
    return updateBrokerRecordVotes

def setUpdateBrokerRecord(record):
    global newUpdateBrokerRecord
    newUpdateBrokerRecord = record
    return newUpdateBrokerRecord
def getUpdateBrokerRecord():
    global newUpdateBrokerRecord
    return newUpdateBrokerRecord
    
    
    
    
    
def getBrokerEpoch(id,BrokerMetadata):
    print(BrokerMetadata)
    for broker in BrokerMetadata["Records"]:
        if id == broker["fields"]["brokerId"]:
            return broker["epoch"]
        
    return "0"
    



def set_log_id():
    filename = 'log'+str(node.nid)+'.txt'
    with open(filename, 'r') as file:
        lines = file.read().splitlines()
        try:
            last_line = lines[-1]
        except:
            return 0
        print (last_line)
    if last_line == '':
        return 0
    else:
        return last_line[8]
    
    

MetaData = dict()
MetaData['RegisterBrokerRecord'] = dict()
MetaData['RegisterBrokerRecord']['Records'] = []
MetaData['RegisterBrokerRecord']['timestamp'] = ''

MetaData['TopicRecord'] = dict()
MetaData['TopicRecord']['Records'] = []
MetaData['TopicRecord']['timestamp'] = ''

MetaData['PartitionRecord'] = dict()
MetaData['PartitionRecord']['Records'] = []
MetaData['PartitionRecord']['timestamp'] = ''

MetaData['ProducerIdsRecord'] = dict()
MetaData['ProducerIdsRecord']['Records'] = []
MetaData['ProducerIdsRecord']['timestamp'] = ''

# MetaData['BrokerRegistrationChangeBrokerRecord'] = dict()
# MetaData['BrokerRegistrationChangeBrokerRecord']['Records'] = []
# MetaData['BrokerRegistrationChangeBrokerRecord']['timestamp'] = ''
MetaData['timestamp']=''


#Broker creation 

@app.route('/createBrokerFromClient',methods=['POST','GET'])
def create_broker():
    setCreateBrokerVotes()
    client_data = json.loads(request.data.decode())
    
    data = dict()
    data["internalUUID"] = str(getBrokerID())
    data["brokerId"] = client_data["brokerId"]
    data["brokerHost"] = client_data["brokerHost"]
    data["securityProtocol"] = client_data["securityProtocol"]
    data["brokerStatus"] = 'active'
    data["rackId"] = client_data["rackId"]
    data["epoch"] = '0'
    data["timestamp"] = str(datetime.datetime.now())
    
    setNewBrokerRecord(data)
    # ID 
    success = False
    for peer in node.peers.values():
        url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/createBrokerRecordFromLeader'
        # try:
        r = requests.post(url=url,data=bytes(str(data),'ascii'))
        if r.content.decode() == "success":
            success = True
        # except:
            # print("Not able to forward to peer ",peer.nid)
        if success:
            return Response(str(data["internalUUID"]))
    return Response("Couldn't create Broker")
@app.route('/confirmCreationBrokerRecord',methods =['POST'])
def confirmBrokerCreation():
    no_of_nodes = len(node.peers) + 1 
    # print(no_of_nodes)
    votes = incrementCreateBrokerVotes()
    data = getNewBrokerRecord()
    confirm = False
    
    if votes == math.ceil((len(node.peers)+1)/2) :# count the no of nodes and make it generalized 
        # try:
        recieved_data = request.data.decode()
        dictionary = dict()
        recieved_data = recieved_data.strip()[1:-1].split(",")
        for ele in recieved_data:
            dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
        data = dict()
        data["type"] = "metadata"
        data["name"] = "RegisterBrokerRecord"
        data["fields"] = dict()
        data["fields"]["internalUUID"] = dictionary["internalUUID"] 
        data["fields"]["brokerId"] = int(dictionary["brokerId"]) 
        data["fields"]["brokerHost"] = dictionary["brokerHost"] 
        data["fields"]["securityProtocol"] = dictionary["securityProtocol"] 
        data["fields"]["brokerStatus"] = dictionary["brokerStatus"] 
        data["fields"]["rackId"] = dictionary["rackId"] 
        data["fields"]["epoch"] = int(dictionary["epoch"] )
        print(data)
        MetaData['RegisterBrokerRecord']['Records'].append(data)
        MetaData['RegisterBrokerRecord']['timestamp'] = dictionary["timestamp"]
        MetaData['timestamp']=dictionary["timestamp"]
        # print(MetaData)
        # trigger add logs
        confirm = True
        incrementBrokerID()
        url = 'http://127.0.1.1:'+str(node.port+1)+'/brokers'
        data_log = dict()
        data_log["Records"] = data
        data_log["timestamp"] = dictionary["timestamp"]
        r = requests.post(url=url,data=bytes(str(data_log),'ascii'))
        # except:
        #     print("couldn't commit")
        if confirm:
            return Response("commited")
    return Response('not commited') # should unique ID of the broker 

@app.route('/createBrokerRecordFromLeader',methods = ['POST'])
def replicateCreateBroker():
    try:
        recieved_data = request.data.decode()
        dictionary = dict()
        recieved_data = recieved_data.strip()[1:-1].split(",")
        for ele in recieved_data:
            dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
        data = dict()
        data["type"] = "metadata"
        data["name"] = "RegisterBrokerRecord"
        data["fields"] = dict()
        data["fields"]["internalUUID"] = dictionary["internalUUID"] 
        data["fields"]["brokerId"] = int(dictionary["brokerId"] )
        data["fields"]["brokerHost"] = dictionary["brokerHost"] 
        data["fields"]["securityProtocol"] = dictionary["securityProtocol"] 
        data["fields"]["brokerStatus"] = dictionary["brokerStatus"] 
        data["fields"]["rackId"] = dictionary["rackId"] 
        data["fields"]["epoch"] = int(dictionary["epoch"] )
        MetaData['RegisterBrokerRecord']['Records'].append(data)
        MetaData['RegisterBrokerRecord']['timestamp'] = dictionary["timestamp"]
        MetaData['timestamp']=dictionary["timestamp"]
        recieved = False
        for peer in node.peers.values():
            if peer.state == 'l':  
                url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/confirmCreationBrokerRecord'
                data = b'Broker Creation done'
                r = requests.post(url=url,data=request.data)
                if r.content.decode() == "commited":
                    recieved = True
    except:
        print("couldn't confirm from :",node.nid)
    # print("Metadata == ",MetaData)
    if recieved:
        return Response("success")
    return Response("failure")




# Topic Creation 

@app.route('/createTopicFromClient',methods=['POST','GET'])
def create_topic():
    setCreateTopicVotes()
    # print("Client requested to create new topic :",request.data.decode())
    # Alter the recieved data here
    client_data = json.loads(request.data.decode())
    
    data = dict()
    data["internalUUID"] = str(getTopicID()) # generate madu 
    data["name"] = client_data["name"]
    data["timestamp"] = str(datetime.datetime.now())
    
    setNewTopic(data)
    # ID 
    success = False
    
    for peer in node.peers.values():
        url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/createTopicFromLeader'
        try:
            r = requests.post(url=url,data=bytes(str(data),'ascii'))
            if r.content.decode() =="success":
                success = True
        except:
            print("Not able to forward to peer ",peer.nid)
    if success:
        return Response(str(data["internalUUID"]))
    return Response("New topic creation forwarded to followers")  
   
@app.route('/createTopicFromLeader',methods = ['POST'])
def replicateCreateTopic():
    # print(request.data.decode())
    # try:
    recieved_data = request.data.decode()
    # data = json.loads(data)
    dictionary = dict()
    # print(recieved_data)
    recieved_data = recieved_data.strip()[1:-1].split(",")
    for ele in recieved_data:
        dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
    data = dict()
    data["type"] = "metadata"
    data["name"] = "TopicRecord"
    data["fields"] = dict()
    data["fields"]["internalUUID"] = dictionary["internalUUID"] 
    data["fields"]["name"] = dictionary["name"] 
    # print(data)
    MetaData['TopicRecord']['Records'].append(data)
    MetaData['TopicRecord']['timestamp'] = dictionary["timestamp"]
    MetaData['timestamp']=dictionary["timestamp"]
    success = False
    for peer in node.peers.values():
        if peer.state == 'l':  
            url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/confirmTopicCreation'
            r = requests.post(url=url,data=request.data)
            if r.content.decode() == "commited":
                success = True
    # except:
        # print("couldn't confirm from :",node.nid)
    # print("Metadata == ",MetaData)
    if success:
        return Response("success")
    return Response("failure")

@app.route('/confirmTopicCreation',methods =['POST'])
def confirmTopicCreation():
    no_of_nodes = len(node.peers) + 1 
    # print(no_of_nodes)
    votes = incrementCreateTopicVotes()
    data = getNewTopic()
    confirmed = False
    if votes == math.ceil((len(node.peers)+1)/2) :# count the no of nodes and make it generalized 
        try:
            recieved_data = request.data.decode()
            dictionary = dict()
            recieved_data = recieved_data.strip()[1:-1].split(",")
            for ele in recieved_data:
                dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
            data = dict()
            data["type"] = "metadata"
            data["name"] = "TopicRecord"
            data["fields"] = dict()
            data["fields"]["internalUUID"] = dictionary["internalUUID"] 
            data["fields"]["name"] = dictionary["name"] 
            MetaData['TopicRecord']['Records'].append(data)
            MetaData['TopicRecord']['timestamp'] = dictionary["timestamp"]
            MetaData['timestamp']=dictionary["timestamp"]
            url = 'http://127.0.1.1:'+str(node.port+1)+'/brokers'
            data_log = dict()
            data_log["Records"] = data
            data_log["timestamp"] = dictionary["timestamp"]
            r = requests.post(url=url,data=bytes(str(data_log),'ascii'))
            confirmed = True
            incrementTopicID()
        except:
            print("couldn't commit")
        if confirmed:
            return Response("commited")
    return Response('Not Commited') # should unique ID of the Topic 



# Create partitions 
   
@app.route('/createPartitionFromClient',methods=['POST','GET'])
def createPartition():
    setCreateNewPartionsVotes()
    print("Client requested to create new Partition :",request.data.decode())
    # Alter the recieved data here
    client_data = json.loads(request.data.decode())
    data = dict()
    data["topicUUID"] = client_data["topicUUID"]
    data["partitionId"] = client_data["partitionId"]
    data["replicas"] = client_data["replicas"]
    data["ISR"] = client_data["ISR"]
    data["removingReplicas"] = client_data["removingReplicas"]
    data["addingReplicas"] = client_data["addingReplicas"]
    data["leader"] = client_data["leader"]
    data["partitionEpoch"] = '0'
    data["timestamp"] = str(datetime.datetime.now())
    setNewPartition(data)
    # ID 
    success = False
    for peer in node.peers.values():
        url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/createPartitionFromLeader'
        try:
            r = requests.post(url=url,data=bytes(str(data),'ascii'))
            if r.content.decode() == "success":
                success = True
        except:
            print("Not able to forward to peer ",peer.nid)
    if success:
        return Response(str(data["partitionId"]))
    return Response("Couldn't create topic")  
   
@app.route('/createPartitionFromLeader',methods = ['POST'])
def replicateCreatepartition():
    print(request.data.decode())
    success = False
    try:
        recieved_data = request.data.decode()
        # data = json.loads(data)
        dictionary = dict()
        recieved_data = recieved_data.strip()[1:-1].split(",")
        for ele in recieved_data:
            dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
        data = dict()
        data["type"] = "metadata"
        data["name"] = "PartitionRecord"
        data["fields"] = dict()
        data["fields"]["topicUUID"] = dictionary["topicUUID"] 
        data["fields"]["partitionId"] = dictionary["partitionId"] 
        data["fields"]["replicas"] = dictionary["replicas"] 
        data["fields"]["ISR"] = dictionary["ISR"] 
        data["fields"]["removingReplicas"] = dictionary["removingReplicas"] 
        data["fields"]["addingReplicas"] = dictionary["addingReplicas"] 
        data["fields"]["leader"] = dictionary["leader"] 
        data["fields"]["partitionEpoch"] = dictionary["partitionEpoch"] 
        # print(data)
        MetaData['PartitionRecord']['Records'].append(data)
        MetaData['PartitionRecord']['timestamp'] = dictionary["timestamp"]
        MetaData['timestamp']=dictionary["timestamp"]
        # print(MetaData)

        for peer in node.peers.values():
            if peer.state == 'l':  
                url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/confirmPartitionCreation'
                r = requests.post(url=url,data=request.data)
                if r.content.decode() == "commited":
                    success = True
    except:
        print("couldn't confirm from :",node.nid)
    print("Metadata == ",MetaData)
    if success:
        return Response("success")
    return Response("failure")

@app.route('/confirmPartitionCreation',methods =['POST'])
def confirmPartitionCreation():
    no_of_nodes = len(node.peers) + 1 
    # print(no_of_nodes)
    votes = incrementeNewPartionsVotes()
    data = getNewPartition()
    confirm = False
    if votes == math.ceil((len(node.peers)+1)/2) :# count the no of nodes and make it generalized 
        try:
            recieved_data = request.data.decode()
            # data = json.loads(data)
            dictionary = dict()
            recieved_data = recieved_data.strip()[1:-1].split(",")
            for ele in recieved_data:
                dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
            data = dict()
            data["type"] = "metadata"
            data["name"] = "PartitionRecord"
            data["fields"] = dict()
            data["fields"]["topicUUID"] = dictionary["topicUUID"] 
            data["fields"]["partitionId"] = dictionary["partitionId"] 
            data["fields"]["replicas"] = dictionary["replicas"] 
            data["fields"]["ISR"] = dictionary["ISR"] 
            data["fields"]["removingReplicas"] = dictionary["removingReplicas"] 
            data["fields"]["addingReplicas"] = dictionary["addingReplicas"] 
            data["fields"]["leader"] = dictionary["leader"] 
            data["fields"]["partitionEpoch"] = dictionary["partitionEpoch"] 
            # print(data)
            MetaData['PartitionRecord']['Records'].append(data)
            MetaData['PartitionRecord']['timestamp'] = dictionary["timestamp"]
            MetaData['timestamp']=dictionary["timestamp"]
            print(MetaData)
            # trigger add logs
            incrementPartitionID()
            url = 'http://127.0.1.1:'+str(node.port+1)+'/brokers'
            data_log = dict()
            data_log["Records"] = data
            data_log["timestamp"] = dictionary["timestamp"]
            r = requests.post(url=url,data=bytes(str(data_log),'ascii'))
            confirm = True
        except:
            print("couldn't commit")
        if confirm:
            return Response("commited")
    return Response("Couldn't commit") # should unique ID of the partition




# ProducerIdsRecord
   
@app.route('/createProducerFromClient',methods=['POST','GET'])
def createProducer():
    setCreateNewPartionsVotes()
    print("Client requested to create new Producer :",request.data.decode())
    # Alter the recieved data here
    client_data = json.loads(request.data.decode())
    print("Meta Data ======================",MetaData,client_data["brokerId"])
    data = dict()
    data["brokerId"] = client_data["brokerId"]
    data["brokerEpoch"] = getBrokerEpoch(client_data["brokerId"],MetaData["RegisterBrokerRecord"])
    data["producerId"] = client_data["producerId"]
    data["timestamp"] = str(datetime.datetime.now())
    
    
    setNewProducer(data)
    # ID 
    for peer in node.peers.values():
        url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/createProducerFromLeader'
        try:
            r = requests.post(url=url,data=bytes(str(data),'ascii'))
        except:
            print("Not able to forward to peer ",peer.nid)
    return Response("New Producer creation forwarded to followers")  
   
@app.route('/createProducerFromLeader',methods = ['POST'])
def replicateCreateProducer():
    print(request.data.decode())
    try:
        recieved_data = request.data.decode()
        dictionary = dict()
        recieved_data = recieved_data.strip()[1:-1].split(",")
        for ele in recieved_data:
            # print("--------------------------------------",ele)
            dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
        data = dict()
        data["type"] = "metadata"
        data["name"] = "PartitionRecord"
        data["fields"] = dict()
        data["fields"]["brokerId"] = dictionary["brokerId"] 
        data["fields"]["brokerEpoch"] = dictionary["brokerEpoch"] 
        data["fields"]["producerId"] = dictionary["producerId"] 
        # print(data)
        MetaData['PartitionRecord']['Records'].append(data)
        MetaData['PartitionRecord']['timestamp'] = dictionary["timestamp"]
        MetaData['timestamp']=dictionary["timestamp"]
        for peer in node.peers.values():
            if peer.state == 'l':  
                url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/confirmProducerCreation'
                data = b'Producer Creation done'
                r = requests.post(url=url,data=request.data)
    except:
        print("couldn't confirm from :",node.nid)
    print("Metadata == ",MetaData)
    return Response("Producer Replicated")

@app.route('/confirmProducerCreation',methods =['POST'])
def confirmProducerCreation():
    no_of_nodes = len(node.peers) + 1 
    # print(no_of_nodes)
    votes = incrementeNewPartionsVotes()
    data = getNewProducer()
    if votes == math.ceil((len(node.peers)+1)/2) :# count the no of nodes and make it generalized 
        try:
            recieved_data = request.data.decode()
            dictionary = dict()
            recieved_data = recieved_data.strip()[1:-1].split(",")
            for ele in recieved_data:
                dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
            data = dict()
            data["type"] = "metadata"
            data["name"] = "PartitionRecord"
            data["fields"] = dict()
            data["fields"]["brokerId"] = dictionary["brokerId"] 
            data["fields"]["brokerEpoch"] = dictionary["brokerEpoch"] 
            data["fields"]["producerId"] = dictionary["producerId"] 
            # print(data)
            MetaData['PartitionRecord']['Records'].append(data)
            MetaData['PartitionRecord']['timestamp'] = dictionary["timestamp"]
            MetaData['timestamp']=dictionary["timestamp"]
            print(MetaData)
            # trigger add logs
            
            url = 'http://127.0.1.1:'+str(node.port+1)+'/brokers'
            data_log = dict()
            data_log["Records"] = data
            data_log["timestamp"] = dictionary["timestamp"]
            r = requests.post(url=url,data=bytes(str(data_log),'ascii'))
            
            
        except:
            print("couldn't commit")
    return Response('Commited Producer Creation') # should unique ID of the Producer
   

@app.route('/UpdateBrokerRecordFromClient',methods = ['POST'])
def updateBrokerRecordFromClient():
    setUpdateBrokerRecordVotes()
    # setCreateNewPartionsVotes()
    print("Client requested to create new Producer :",request.data.decode())
    # Alter the recieved data here
    client_data = json.loads(request.data.decode())
    data = dict()
    data["brokerId"] = client_data["brokerId"]
    data["brokerHost"] = client_data["brokerHost"]
    # data["brokerPort"] = client_data["brokerPort"]
    data["securityProtocol"] = client_data["securityProtocol"]
    data["brokerStatus"] = client_data["brokerStatus"]
    data["timestamp"] = str(datetime.datetime.now())
    
    
    setUpdateBrokerRecord(data)
    # ID 
    success = False
    for peer in node.peers.values():
        url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/updateBrokerRecordFromLeader'
        # try:
        r = requests.post(url=url,data=bytes(str(data),'ascii'))
        if r.content.decode() == "success":
            success = True
        # except:
            # print("Not able to forward to peer ",peer.nid)
        if success:
            return Response("successfully updated")
    return Response("Couldn't create Broker")

@app.route('/confirmBrokerRecordUpdation',methods =['POST'])
def confirmBrokerupdation():
    no_of_nodes = len(node.peers) + 1 
    # print(no_of_nodes)
    votes = incrementUpdateBrokerRecordVotes()
    data = getUpdateBrokerRecord()
    confirm = False
    
    if votes == math.ceil((len(node.peers)+1)/2) :# count the no of nodes and make it generalized 
        # try:
        recieved_data = request.data.decode()
        dictionary = dict()
        recieved_data = recieved_data.strip()[1:-1].split(",")
        for ele in recieved_data:
            dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
        data = dict()
        data["type"] = "metadata"
        data["name"] = "RegistrationChangeBrokerRecord"
        data["fields"] = dict()
        data["fields"]["brokerId"] = int(dictionary["brokerId"]) 
        data["fields"]["brokerHost"] = dictionary["brokerHost"] 
        data["fields"]["securityProtocol"] = dictionary["securityProtocol"] 
        data["fields"]["brokerStatus"] = dictionary["brokerStatus"] 
        print(data)
        # MetaData['RegistrationChangeBrokerRecord']['Records'].append(data)
        # MetaData['RegistrationChangeBrokerRecord']['timestamp'] = dictionary["timestamp"]
        # MetaData['RegisterBrokerRecord']['timestamp'] = dictionary["timestamp"]
        
        for record in MetaData["RegisterBrokerRecord"]["Records"]:
            if int(dictionary["brokerId"]) == record["fields"]["brokerId"]:
                record["fields"]["brokerHost"] = dictionary["brokerHost"]
                record["fields"]["securityProtocol"] = dictionary["securityProtocol"]
                record["fields"]["brokerStatus"] = dictionary["brokerStatus"]
                record["fields"]["epoch"] = str(int(record["fields"]["epoch"])+1)
                MetaData['timestamp']=dictionary["timestamp"]
                

        
        # print(MetaData)
        # trigger add logs
        confirm = True
        url = 'http://127.0.1.1:'+str(node.port+1)+'/brokers'
        data_log = dict()
        data_log["Records"] = data
        data_log["timestamp"] = dictionary["timestamp"]
        r = requests.post(url=url,data=bytes(str(data_log),'ascii'))
        # except:
        #     print("couldn't commit")
        if confirm:
            return Response("commited")
    return Response('not commited') # should unique ID of the broker 

@app.route('/updateBrokerRecordFromLeader',methods = ['POST'])
def replicateUpdateBroker():
    try:
        recieved_data = request.data.decode()
        dictionary = dict()
        recieved_data = recieved_data.strip()[1:-1].split(",")
        for ele in recieved_data:
            dictionary[ele.strip().split(":",1)[0][1:-1]] = ele.strip().split(":",1)[1].strip()[1:-1]
        data = dict()
        data["type"] = "metadata"
        data["name"] = "RegistrationChangeBrokerRecord"
        data["fields"] = dict()
        data["fields"]["brokerId"] = int(dictionary["brokerId"]) 
        data["fields"]["brokerHost"] = dictionary["brokerHost"] 
        data["fields"]["securityProtocol"] = dictionary["securityProtocol"] 
        data["fields"]["brokerStatus"] = dictionary["brokerStatus"]
        print(data)
        # MetaData['RegistrationChangeBrokerRecord']['Records'].append(data)
        # MetaData['RegistrationChangeBrokerRecord']['timestamp'] = dictionary["timestamp"]
        # MetaData['RegisterBrokerRecord']['timestamp'] = dictionary["timestamp"]
        
        for record in MetaData["RegisterBrokerRecord"]["Records"]:
            if int(dictionary["brokerId"]) == record["fields"]["brokerId"]:
                record["fields"]["brokerHost"] = dictionary["brokerHost"]
                record["fields"]["securityProtocol"] = dictionary["securityProtocol"]
                record["fields"]["brokerStatus"] = dictionary["brokerStatus"]
                record["fields"]["epoch"] = str(int(record["fields"]["epoch"])+1) 
                MetaData['timestamp']=dictionary["timestamp"]
                
        recieved = False
        for peer in node.peers.values():
            if peer.state == 'l':  
                url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/confirmBrokerRecordUpdation'
                data = b'Broker Creation done'
                r = requests.post(url=url,data=request.data)
                if r.content.decode() == "commited":
                    recieved = True
    except:
        print("couldn't confirm from :",node.nid)
    # print("Metadata == ",MetaData)
    if recieved:
        return Response("success")
    return Response("failure")


@app.route('/GetAllActiveBrokers',methods=['POST'])
def GetAllActiveBrokers():
    result = []
    for records in MetaData["RegisterBrokerRecord"]["Records"]:
        if records["fields"]["brokerStatus"] == 'active':
            result.append(records)
    result = str(result)
    url = 'http://127.0.1.1:'+str(node.port+1)+'/brokers'
    r = requests.post(url=url,data=bytes(str(result),'ascii'))
    return Response(bytes(result,'ascii'))

@app.route('/GetBrokerById',methods= ['POST'])
def GetBrokerById():
    result = ''
    requested_id = request.data.decode()
    
    for records in MetaData["RegisterBrokerRecord"]["Records"]:
        print(requested_id,records["fields"]["brokerId"])
        if int(records["fields"]["brokerId"]) == int(requested_id):
            result = records
    url = 'http://127.0.1.1:'+str(node.port+1)+'/brokers'
    r = requests.post(url=url,data=bytes(str(result),'ascii'))
    return Response(bytes(str(result),'ascii'))

@app.route('/GetTopicBYId',methods=['POST'])
def GetTopicById():
    result = ''
    requested_name = request.data.decode()
    
    
    for records in MetaData["TopicRecord"]["Records"]:
        print(requested_name,records["fields"]["internalUUID"])
        if records["fields"]["name"] == requested_name:
            result = records
    url = 'http://127.0.1.1:'+str(node.port+1)+'/brokers'
    r = requests.post(url=url,data=bytes(str(result),'ascii')) 
    return Response(bytes(str(result),'ascii'))

   
@app.route('/brokers', methods=['POST'])
def get_data_brokers():
    print("reciever data from client :",request.data.decode())
    votes = set_votes()
    set_current_log(str(request.data.decode()))
    log_id = int(get_current_log_id())+1
    for peer in node.peers.values():
        url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/fromLeader'
        try:
            print(log_id,"PEER ID =",peer.nid)
            final_data = str(log_id)+request.data.decode()
            final_data = bytes(final_data,'ascii')
            print(final_data)
            r = requests.post(url=url, data=final_data)
            print("leader",votes)
            # time.sleep(1)
        except:
            print("not able to forward message to follower",peer.nid)
    
    return Response('We recieved something…')

@app.route('/confirmation',methods=['POST','GET'])
def leader_confirm():
    print("confirmation from followers :",request.data.decode())
    no_of_nodes = 1 +len(node.peers)
    print(no_of_nodes)
    votes =increament_votes()
    data = get_current_log()
    if votes == math.ceil((len(node.peers)+1)/2) :# count the no of nodes and make it generalized 
        id = set_log_id()
        update_log_id(id)
        log_id = increament_current_log_id()
        final_data = 'Log Id ('+str(log_id)+') : '+data+'\n'
        filename = 'log'+str(node.nid)+'.txt'
        file = open(filename, "a")  # append mode
        file.write(str(final_data))
        file.close()
        # print("commit madu")
    # print("confirmed madu",request.data,"votes = ",votes)
    return Response('We recieved something…')
@app.route('/fromLeader', methods=['POST'])
def get_data_leader():
    print("recieved message from leader :",request.data.decode())
    time.sleep(1)
    for peer in node.peers.values():
        if(peer.state =='l'):
            confimation_url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/confirmation'
            data = b'data recieved madu'
            # try:
            r = requests.post(url=confimation_url, data=data)
            id = set_log_id()
            update_log_id(id)
            log_id = int(get_current_log_id())+1
            # print("data recieved ",request.data.decode(),int(request.data.decode()[0])," == ",log_id)
            if int(request.data.decode()[0]) == log_id:
                final_data = 'Log Id ('+str(log_id)+') : '+str(request.data.decode()[1:])+'\n'
                filename = 'log'+str(node.nid)+'.txt'
                file = open(filename, "a")  # append mode
                file.write(final_data)
                file.close()
                increament_current_log_id()
            elif int(request.data.decode()[0]) > log_id:
                retry_url = 'http://127.0.1.1:'+str(int(peer.port)+1)+'/leader_retry'
                to_send = str(log_id)+':'+str(node.port)
                offset = bytes(to_send,'ascii')
                r = requests.post(url=retry_url, data=offset)
                print("request for more data")
            else:
                print("waiting for sync...")
    return Response('We recieved something…')


@app.route('/leader_retry',methods =['POST'])
def retry_send():
    data_recieved = request.data.decode().split(":")
    follower_offset = int(data_recieved[0])
    follower_port = data_recieved[1]
    
    # follower_offset = int(request.data.decode())
    print("FOLLOWER OFFSET ==",follower_offset)
    filename = 'log'+str(node.nid)+'.txt'
    file = open(filename,'r')
    content = file.readlines() 
    data_to_be_sent = content[follower_offset-1:]
    print(data_to_be_sent)
    data =bytes(str(data_to_be_sent),'ascii')
    retry_url = 'http://127.0.1.1:'+str(int(follower_port)+1)+'/follower_retry'
    r = requests.post(url=retry_url, data=data)
    return Response('We recieved something…')       
            
@app.route('/follower_retry',methods=['POST'])
def retry_recieve():
    # print(request.data.decode())
    data = request.data.decode()[1:-1]
    lines = data.split(",")
    print(lines)
    filename = 'log'+str(node.nid)+'.txt'
    file = open (filename,"a")
    for line in lines:
        print(line)
        line = line.strip()[1:-3]+'\n'
        file.write(line)
    file.close()
        
    return Response('We recieved something…')

@app.route('/FetchSnapshot',methods = ['POST'])
def sendSnapshot():
    print("recieved request")
    url = 'http://127.0.1.1:6000/recieveSnapshot'
    try:
        r = requests.post(url=url,data=bytes(str(MetaData),'ascii'))
    except:
        print("not able to send snap shot")
    return Response("send Snapshot")

@app.route('/FetchPartialSnapshot',methods = ['POST'])
def sendPartialSnapshot():
    print("recieved request")
    url = 'http://127.0.1.1:7000/recievePartialSnapshot'
    try:
        r = requests.post(url=url,data=bytes(str(MetaData),'ascii'))
    except:
        print("not able to send snap shot")
    return Response("send Snapshot")


def leader_run_madu(node):

    def ping():
        while not node.shutdown_flag:
            time.sleep(2)
            Brokerurl = 'http://127.0.1.1:6000/BrokerHeartBeat'
            Producerurl = 'http://127.0.1.1:7000/ProducerHeartBeat'
            while True:
                data = MetaData['timestamp']+'|'+'http://127.0.1.1:'+str(node.port+1)
                data = bytes(str(data),'ascii')
                try:
                    r = requests.post(url=Brokerurl,data=data)
                except:
                    print("Broker is Down")
                try:
                    r = requests.post(url=Producerurl,data=data)
                except:
                    print("Producer is Down")
    
    x1 = threading.Thread(target=ping)
    x1.start()
    x1.join()

def leader_callback(node):
    print('starting...')
    node = threading.Thread(target=leader_run_madu, args=(node,))
    node.start()
    # node.join()
    
def follower_run_madu(node):
    i = 0
    
    ip = node.ip
    def ping():
        print("called ping")
        while not node.shutdown_flag:
            time.sleep(2)
            print("from follower",node.state)
            for peer in node.peers.values():
                if(peer.state =='l'):
                    # print("leader-address",peer.addr)
                    url = 'http://127.0.1.1:'+str(int(peer.port)+1)
                    data = b'follower alive'

    x1 = threading.Thread(target=ping)

    x1.start()

def follower_callback(node):
    print('starting...')
    node = threading.Thread(target=follower_run_madu, args=(node,))
    node.start()


node = raft.make_default_node()

port = int(node.port)+1
def start_server():
    if get_server_running() == False:
        app.run(debug=False,port=port,host='127.0.1.1')
x4 = threading.Thread(target=start_server)
x4.start()

node.worker.handler['on_leader'] = leader_callback
node.worker.handler['on_follower'] = follower_callback

node.start()
node.join()
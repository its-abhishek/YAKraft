import requests
from flask import Flask, Response, request
from flask_cors import CORS
import time

leader_addr = ''
def getLeaderAddr():
    global leader_addr
    return leader_addr

def setLeaderAddr(addr):
    global leader_addr
    leader_addr = addr
    return leader_addr


Metadata =''
timeStamp = ''

def setTimeStamp(currentTimeStamp):
    global timeStamp 
    timeStamp = currentTimeStamp
    return timeStamp

    
def setMetaData(currData):
    global Metadata
    Metadata = currData
    return Metadata

app = Flask(__name__)
CORS(app)




@app.route('/GetLeaderAddr',methods = ['POST'])
def GetLeaderAddr():
    leaderIP = getLeaderAddr()
    return Response(str(leaderIP))



@app.route('/ProducerHeartBeat',methods=['POST'])
def ProducerHeartBeat():
    time.sleep(2)
    recieved_data = request.data.decode()
    offset = recieved_data.strip().split("|")[0]
    leaderAddr = recieved_data.strip().split("|")[1]
    setLeaderAddr(leaderAddr)
    print(offset.strip() != timeStamp.strip())
    if offset != timeStamp:
        leaderIP = getLeaderAddr()
        url = leaderIP+'/FetchPartialSnapshot'
        
        r = requests.post(url=url,data=b'send MetaData')
        print("requested for metadata")
        
    return Response("recieved meta data")



@app.route('/FetchPartialSnapshot',methods=['POST'])
def recieveSnapshot():
    data = request.data.decode()
    data = data.strip().split(" ")[-2][1:]+" "+data.strip().split(" ")[-1][:-2]
    setTimeStamp(data)
    setMetaData(request.data.decode())

    return Response("Snapshot recieved")


app.run(debug=False,port=7000,host='127.0.1.1')
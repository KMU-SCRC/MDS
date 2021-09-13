import paho.mqtt.client as mqtt
import pandas as pd
import json
import csv
import datetime
import os
import sys
#파일 저장, 도착한 시간이랑 같이 
#callback = 메세지를 받았을 때 실행하는 함수


#mqtt 서버에 정상 접속 확인
def on_connect(client,userdata,flag,rc):
    print("Connect",str(rc))
    print("client",client)
    print("flag= ",flag)

def on_message(client,userdata,message):
    #메시지가 오면 이 함수가 실행
    #json 파일 수신 후 csv로 변경
    # print("message received",str(message.payload.decode("utf-8")))
    print(datetime.datetime.now())

    data = str(message.payload.decode("utf-8"))
    info = json.loads(data)["employees"]
    # print("info : " ,info)
    # print(info[0].keys())
    keylist = []
    for x in info[0].keys():
        keylist.append(x)
    # print(keylist)

    df = pd.DataFrame(info,columns =keylist) #열 추가하기위해 dataframe으로 변경
    df.insert(0,"received time",datetime.datetime.now()) #받은 시간을 추가하기 위해 열 추가
    keylist.append("received time")
    print("df--------------")
    print(df)
    if os.path.isfile('/home/kmuscrc/Desktop/code/jsontocsvtest.csv'): #파일이 존재할 경우 헤더 없이 출력
        csv = df.to_csv("jsontocsvtest.csv",index = False,mode='a',header = False)
    else:
        csv = df.to_csv("jsontocsvtest.csv",index = False,mode='a')
    # js = df.to_json(orient='records')
    # print(js)
    # print(type(js))
    # print(type(js[0]))
    # print(js.keys())

    # ##csv파일로 저장
    # sys.stdout = open('test.csv','a')
    # with open("jsontocsv.csv", 'w') as f: 
    #     wr = csv.writer(f) 
    #     wr.writerow(keylist) 
    #     wr.writerow(js) 

    ###csv 수신시
    # sys.stdout = open('test.csv','a')
    # 확장자에 따라 저장
    # print("message received",str(message.payload.decode("utf-8")))
    # print("message topic=",message.topic)
    # print("message qos=",message.qos)
    # print("message retain flag=",message.retain)
    # print(datetime.datetime.now())
    print("---------------------------")
    # sys.stdout.close()

# def on_subscribe(client,userdatammid,granted_qos):
#     print("subscribed: "+ str(mid)+" "+ str(granted_qos))


broker_address="192.168.50.240"
client1 = mqtt.Client("client1") #구독자 이름
client1.on_connect = on_connect
client1.connect(broker_address) #서버에 접속
client1.subscribe("outTopic") #구독하는 토픽이름
client1.on_message = on_message #callback 함수로 등록
client1.loop_forever() #무한반복  

import paho.mqtt.client as mqtt
from PIL import Image
import csv
import datetime
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
    # message.save('test.png')

    ##csv파일로 변경
    # with open("samplecsv.csv", 'w') as f: 
    #     wr = csv.DictWriter(f, fieldnames = info[0].keys()) 
    #     wr.writeheader() 
    #     wr.writerows(info) 

    ###csv 수신시
    sys.stdout = open('biologging.csv','a')
    #확장자에 따라 저장
    print("message received",str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    print(datetime.datetime.now())
    print("---------------------------")
    sys.stdout.close()

# def on_subscribe(client,userdatammid,granted_qos):
#     print("subscribed: "+ str(mid)+" "+ str(granted_qos))


broker_address="192.168.50.240"
client1 = mqtt.Client("client1") #구독자 이름
client1.on_connect = on_connect
client1.connect(broker_address) #서버에 접속
client1.subscribe("outTopic") #구독하는 토픽이름
client1.on_message = on_message #callback 함수로 등록
client1.loop_forever() #무한반복  

#전달 완료 체크?

#무결성 체크
#체크섬

#포맷적용

import paho.mqtt.client as mqtt
# from PIL import Image
import csv
import datetime
import sys
import os
#파일 저장, 도착한 시간이랑 같이 
#callback = 메세지를 받았을 때 실행하는 함수


#mqtt 서버에 정상 접속 확인
def on_connect(client,userdata,flag,rc):
  print("Connect",str(rc))
  # print("client",client)
  # print("flag= ",flag)

filenum = {}
def on_message(client,userdata,message):
  message = str(message.payload.decode("utf-8"))
  meslist = message.split(':')
  print(meslist)
  
  # f = null
   #파일이 몇번째 줄까지 써졌는지 확인
  if (len(meslist) == 3):
    print("meslist = ", meslist)
    print("filenum = ", filenum)
    print("-------------")
    if os.path.isfile('/home/kmuscrc/Desktop/MDS/'+meslist[0]): #파일이 존재할 경우
        print("YES FILE")
        with open('/home/kmuscrc/Desktop/MDS/'+meslist[0]) as myfile:
            total_lines = sum(1 for line in myfile)
        print(total_lines)
        filenum[meslist[0]] = total_lines + 1 #원하는 건 다음번 줄 
        print(filenum.keys())
        print(filenum.values())
    else:
      print('/home/kmuscrc/Desktop/MDS/'+meslist[0])
      print("NO FILE")
      filenum[meslist[0]] = 1
      print(filenum)

    if (int(meslist[1]) == filenum[meslist[0]]): #행수가 같으면
      print("in",meslist[1])
      f = open(meslist[0], 'a')
      f.write(meslist[2])
      filenum[meslist[0]] += 1
      f.close()
      print(filenum[meslist[0]])
    else :
      print("i dont want this message")
  else:
    print("the other message")
  
    

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

import paho.mqtt.client as mqtt
# from PIL import Image
import csv
import time
import threading
import sys
import os
#파일 저장, 도착한 시간이랑 같이 
#callback = 메세지를 받았을 때 실행하는 함수

secs = time.time()
#mqtt 서버에 정상 접속 확인
def on_connect(client,userdata,flag,rc): #브로커에 연결되는 순간 실행
  print("Connect",str(rc))
  # client1.connect(broker_address) #서버에 접속
  client1.subscribe("BIOLOGGER/#") #토픽 구독
  client1.on_message = on_message #메세지 왔을 때 실행하는 함수
  # if (secs-time.time() >= 10):
  # print("client",client)
  # print("flag= ",flag)

def createFolder(directory): #디렉토리 생성
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
 
data = ""
filenum = {} #파일별로 몇줄까지 썻나 딕셔너리 형태로 관리
fileway = ""
meslist = []

def timecheck():
  global secs
  timer = threading.Timer(1,timecheck)
  print("timecheck")
  if (time.time()-secs >= 10):
    print("time sec 10 passed!")
    print(fileway+'/'+meslist[0])
    f = open(fileway+'/'+meslist[0], 'a')
    f.write(data)
    f.close()
    secs = time.time()+time.time()
  timer.start()

def on_message(client,userdata,message):
  global data
  global meslist
  global fileway
  global secs
  topic = message.topic
  topiclist = topic.split('/')
  fileway = topiclist[0]+'/'+topiclist[1]+'/'+topiclist[2]
  print(fileway)
  message = str(message.payload.decode("utf-8"))
  meslist = message.split(':')
  print(meslist)
  secs = time.time()
  #파일명: num : data
  #파일이 몇번째 줄까지 써졌는지 확인
  if (len(meslist) == 3):
    # print("filenum[meslist[0]] = ", filenum[meslist[0]])
    print("-------------")
    createFolder('/home/kmuscrc/Desktop/MDS/'+fileway)
    if os.path.isfile('/home/kmuscrc/Desktop/MDS/'+fileway+'/'+meslist[0]) and fileway+meslist[0] not in filenum: #파일이 존재할 경우 + 딕셔너리 
        print("YES FILE")
        with open('/home/kmuscrc/Desktop/MDS/'+fileway+'/'+meslist[0]) as myfile: #파일 열어서 몇 번째 줄까지 써져있는지 확인
            total_lines = sum(1 for line in myfile)
        filenum[fileway+meslist[0]] = total_lines + 1 #원하는 건 다음번 줄 
        print(filenum)
    else:
      filenum[fileway+meslist[0]] = 1
    if (int(meslist[1]) == filenum[fileway+meslist[0]]): #행수가 같으면
      if (meslist[2] == 'DONE' or sys.getsizeof(data) >= 500): #getsizeof() 변수에 할당된 바이트 크기 
        f = open(fileway+'/'+meslist[0], 'a')
        f.write(data)
        f.close()
        if (meslist[2] == 'DONE'):
          #  client1.publish(topiclist[3],"DELETE:"+meslist[0]) #파일 수신이 끝났으면 삭제하라고 DELETE 메시지 발행
          print('send DELETE!')
      else:
        data += meslist[2]  
        print(data)
        filenum[fileway+meslist[0]] += 1
    elif meslist[0] == "OK" :
      client1.publish(topiclist[3],"RESTART:"+meslist[0]+":"+filenum[fileway+meslist[0]])
    elif meslist[0] == "READY":
      client1.publish(topiclist[3],"START:")
    else:
      print("i dont want this message")
      client1.publish(topiclist[3],"STOP:")
  else:
    print("the other message")

def on_disconnect(client, userdata, flags, rc=0):
    f = open(fileway+'/'+meslist[0], 'a')
    f.write(data)
    f.close()
    

# def on_subscribe(client,userdatammid,granted_qos):
#     print("subscribed: "+ str(mid)+" "+ str(granted_qos))


broker_address="192.168.50.191"
client1 = mqtt.Client("client1") #구독자 이름
client1.on_connect = on_connect
client1.on_disconnect = on_disconnect
timecheck()
client1.connect(broker_address) #서버에 접속
# client1.subscribe("BIOLOGGER/#") #구독하는 토픽이름
# client1.on_message = on_message #callback 함수로 등록
client1.loop_forever() #무한반복 
# 중간에 콜백함수가 더 이상 실행되지 않을 경우
#전달 완료 체크?

#무결성 체크
#체크섬

#포맷적용

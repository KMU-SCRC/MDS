import paho.mqtt.client as mqtt
# from PIL import Image
import csv
import time
import threading
import sys
import os
#파일 저장, 도착한 시간이랑 같이 
#callback = 메세지를 받았을 때 실행하는 함수
data = ""
filenum = {} #파일별로 몇줄까지 썻나 딕셔너리 형태로 관리
fileway = ""
meslist = []
secs = time.time()
gateway = 0
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

def timecheck(): #시간 체크 10초 이상 경과시 파일 저장
  global secs
  global gateway
  global data
  timer = threading.Timer(1,timecheck) #1초마다 해당 함수 실행
  print("timecheck")
  if (time.time()-secs >= 10 and gateway):
    print("time sec 10 passed!")
    f = open(fileway+'/'+meslist[0], 'a')
    f.write(data)
    f.close()
    gateway = 0
    data = ""
  timer.start()

def on_message(client,userdata,message):
  global data
  global meslist
  global fileway
  global secs
  global gateway
  topic = message.topic
  topiclist = topic.split('/')
  fileway = topiclist[0]+'/'+topiclist[1]+'/'+topiclist[2]
  message = str(message.payload.decode("utf-8"))
  meslist = message.split(':')
  # print(meslist)
  secs = time.time()
  gateway = 1 #1 메시지 받았다.
  senddone = 0 # 0 안받았다. 1 받았다.
  #파일명: num : data
  #파일이 몇번째 줄까지 써졌는지 확인
  
  if (len(meslist) == 3):
    # print("filenum[meslist[0]] = ", filenum[meslist[0]])
    print("-------------")
    createFolder('/home/kmuscrc/Desktop/MDS/'+fileway)
    if os.path.isfile('/home/kmuscrc/Desktop/MDS/'+fileway+'/'+meslist[0]) and fileway+"/"+meslist[0] not in filenum: #파일이 존재하고 딕셔너리에 없을 때 
      print("YES FILE")
      with open('/home/kmuscrc/Desktop/MDS/'+fileway+'/'+meslist[0]) as myfile: #파일 열어서 몇 번째 줄까지 써져있는지 확인
          total_lines = sum(1 for line in myfile)
      filenum[fileway+'/'+meslist[0]] = total_lines + 1 #원하는 건 다음번 줄 
      print(filenum)
    elif fileway+"/"+meslist[0] not in filenum: #처음 받을 때
      filenum[fileway+"/"+meslist[0]] = 1
    if (int(meslist[1]) == filenum[fileway+"/"+meslist[0]]): #행수가 같으면
      print("행수가 같아")
      if meslist[2] == 'DONE': #getsizeof() 변수에 할당된 바이트 크기 
        senddone = 1
        f = open(fileway+'/'+meslist[0], 'a')
        f.write(data)
        f.close()
        data = ""
        client1.publish(topiclist[3],"DELETE:"+meslist[0]) #파일 수신이 끝났으면 삭제하라고 DELETE 메시지 발행
        print('send DELETE!')
        # print("파일에 썻어")
      elif sys.getsizeof(data) >= 1000:
        print("data에 저장")
        data += meslist[2]  
        print("data : ",data)
        filenum[fileway+"/"+meslist[0]] += 1
        f = open(fileway+'/'+meslist[0], 'a')
        f.write(data)
        f.close()
        data = ""
      else:
        print("data에 저장")
        data += meslist[2]  
        print("data : ",data)
        filenum[fileway+"/"+meslist[0]] += 1    
    else:
      print("i dont want this message")
      client1.publish(topiclist[3],"STOP:"+meslist[0])
      print("stop",meslist[0])
  else:
    if meslist[0] == "OK" : #stop수신후 다시 보내는 거 ok
      client1.publish(topiclist[3],"RESTART:"+meslist[1]+":"+str(filenum[fileway+"/"+meslist[1]]))
      print("OK")
    elif meslist[0] == "START_READY": #1번부터 보내려고 준비
      client1.publish(topiclist[3],"START:")
      print("START_READY")
    elif meslist[0] == "DELETE_READY": #다 보내고 삭제 대기
      print("DELETE_READY")
      # print(meslist)
      # print(fil)
      if senddone == 1:
        client1.publish(topiclist[3],"DELETE:"+meslist[1])
        print("DELETE")
      else: 
        client1.publish(topiclist[3],"RESTART:"+meslist[1]+":"+str(filenum[fileway+"/"+meslist[1]]))
        print("RESTART")
    

def on_disconnect(client, userdata, flags, rc=0): #브로커와 연결이 끊어졌을 때
  global data
  global fileway
  global meslist
  f = open(fileway+'/'+meslist[0], 'a')
  f.write(data)
  f.close()
  data = ""
    

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

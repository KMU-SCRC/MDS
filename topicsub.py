import paho.mqtt.client as mqtt
# from PIL import Image
import csv
import datetime
import sys
import os
#파일 저장, 도착한 시간이랑 같이 
#callback = 메세지를 받았을 때 실행하는 함수

#mqtt 서버에 정상 접속 확인
def on_connect(client,userdata,flag,rc): #브로커에 연결되는 순간 실행
  print("Connect",str(rc))
  # client1.connect(broker_address) #서버에 접속
  client1.subscribe("BIOLOGGER/#") #토픽 구독
  client1.on_message = on_message #메세지 왔을 때 실행하는 함수
  # print("client",client)
  # print("flag= ",flag)

def createFolder(directory): #디렉토리 생성
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
 

filenum = {} #파일별로 몇줄까지 썻나 딕셔너리 형태로 관리
def on_message(client,userdata,message):
  message = str(message.payload.decode("utf-8"))
  meslist = message.split(':') # 아이디 : 토픽 : 파일명 : 줄 수 : 데이터
  print(meslist)
  
  # 메시지 형식  ID:biologger/penguin:파일명:num:data
   #파일이 몇번째 줄까지 써졌는지 확인
  if (len(meslist) == 5):
    print("meslist = ", meslist)
    # print("meslist[1] = ", meslist[1])
    print("filenum = ", filenum)
    # print("filenum[meslist[0]] = ", filenum[meslist[0]])
    print("-------------")
    createFolder('/home/kmuscrc/Desktop/MDS/'+meslist[1])
    if os.path.isfile('/home/kmuscrc/Desktop/MDS/'+meslist[1]+'/'+meslist[2]): #파일이 존재할 경우
        print("YES FILE")
        with open('/home/kmuscrc/Desktop/MDS/'+meslist[1]+'/'+meslist[2]) as myfile: #파일 열어서 몇 번째 줄까지 써져있는지 확인
            total_lines = sum(1 for line in myfile)
        print(total_lines)
        filenum[meslist[1]+'/'+meslist[2]] = total_lines + 1 #원하는 건 다음번 줄 
        print(filenum.keys())
        print(filenum.values())
    else:
      print('/home/kmuscrc/Desktop/MDS/'+meslist[1]+'/'+meslist[2]) #파일이 없는 경우
      print("NO FILE")
      filenum[meslist[1]+'/'+meslist[2]] = 1 
      print(filenum)

    if (int(meslist[3]) == filenum[meslist[1]+'/'+meslist[2]]): #행수가 같으면
      if (meslist[4] != 'DONE'):
        print("in",meslist[4])
        f = open(meslist[1]+'/'+meslist[2], 'a')
        f.write(meslist[4])
        filenum[meslist[1]+'/'+meslist[2]] += 1
        f.close()
        print(filenum[meslist[1]+'/'+meslist[2]])
      else:
        client1.publish(meslist[0],"DELETE:"+meslist[2]) #파일 수신이 끝났으면 삭제하라고 DELETE 메시지 발행
        print('send DELETE!!!!!!!!!!!!!!!!!!!!')
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
# client1.subscribe("BIOLOGGER/#") #구독하는 토픽이름
# client1.on_message = on_message #callback 함수로 등록
client1.loop_forever() #무한반복  연결이 끊겼다가 다시 연결시 콜백함수 안됨..
# 중간에 콜백함수가 더 이상 실행되지 않을 경우
#전달 완료 체크?

#무결성 체크
#체크섬

#포맷적용

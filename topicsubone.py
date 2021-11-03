import paho.mqtt.client as mqtt
from datetime import datetime
import os
def on_connect(client,userdata,flag,rc):
    """브로커에 연결되었을 때 실행되는 함수
    """
    print("Connect",str(rc))
    client1.subscribe("BIOLOGGER/#") # 토픽 구독
    client1.on_message = on_message # 메세지 왔을 때 실행하는 함수 콜백함수로 등록

def createFolder(directory):
    """디렉토리 생성하는 함수

    Args:
        directory (string) : 저장하길 원하는 경로 
    """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

perform = {} # 성능 체크하는 딕셔너리 // key[토픽/파일명] : value([받은시간, stop 받은 횟수, restart 받은 횟수, 재시작 횟수, 끝난시각])
senddone = {} # DONE 보냈는지 BOOL로 관리 // key[토픽/파일명] : value( bool )
filenum = {} # 받은 줄 수 관리 // key[토픽/파일명] : value(저장된 줄 수)
restartcheck = {} # 성능 체크시 재시작 횟수 체크를 위한 bool //key[토픽/파일명] : value( bool ) 
thefile = "" # 토픽/파일명


def on_message(client,userdata,message):
    """메시지 왔을 때 실행되는 함수

    Args:
        client (int, hex()) : 받는 client 고유값
        userdata (int, hex()) : 보내는 client 고유값
        message (class) : 받는 message 값
    """
    global perform
    global senddone
    global filenum
    global thefile
    global restartcheck
    topiclist = message.topic.split('/') # message.topic = BIOLOGGER/PENGUIN/1/고유번호
    fileway = topiclist[0]+'/'+topiclist[1]+'/'+topiclist[2] # 디렉토리 경로 변수에 저장
    #   print(topiclist[3]) # 고유번호
    message = message.payload.decode("1250") # 1250 메시지 디코드
    meslist = message.split(':') # message = 파일명 : num : data, message = 파일명 : OK,DELETE_READY
    thefile = fileway+"/"+meslist[0]
    # print(thefile)

    if (len(meslist) == 3): # 데이터와 명령어 구분 (3개시 데이터)
        createFolder('/media/kmuscrc/5A3C74053C73DB09/'+fileway)

        if os.path.isfile('/media/kmuscrc/5A3C74053C73DB09/'+thefile) and thefile not in filenum and thefile not in senddone: # 파일이 존재할 때
            print("파일이 존재")
            senddone[thefile] = False #파일이 존재하고 딕셔너리에 없을 때
            with open('/media/kmuscrc/5A3C74053C73DB09/'+thefile) as myfile: # 파일 열어서 몇 번째 줄까지 써져있는지 확인
                total_lines = sum(1 for line in myfile)
            filenum[thefile] = total_lines + 1 # 원하는 건 다음번 줄
            # perform[thefile] = [datetime.datetime.now(),0,0,0,datetime.datetime.now()] # 받은시간 ,stop,restart,전체 재시작,끝난시각
            restartcheck[thefile] = False
            print(perform)

        elif thefile not in filenum: # 처음 받을 때
            filenum[thefile] = 1
            senddone[thefile] = False
            perform[thefile] = [datetime.now(),0,0,0,datetime.now()]
            restartcheck[thefile] = False
            print(perform)

        if (int(meslist[1]) == filenum[thefile]): # 행수가 같으면
            print(thefile,meslist[1])
            if restartcheck[thefile] : # 재시도 횟수 체크
                print("restartcheck")
                perform[thefile][3] += 1
                restartcheck[thefile] = False

            if meslist[2] == 'DONE':
                print("DONE")
                senddone[thefile] = True
                client1.publish(topiclist[3],"DELETE:"+meslist[0]) # 파일 수신이 끝났으면 DELETE 메시지 발행
                print('send DELETE!')
                print("perform items",perform.items())
                perform[thefile][4] = datetime.now()
                with open("perform.txt",'a') as fw: #성능 딕셔너리 파일에 저장                    
                    for key,value in perform.items():
                        if key == thefile:
                            # print(key)
                            fw.write(f'{key} : {value}\n')
            else:
                # 파일에 저장 
                filenum[thefile] += 1
                f = open('/media/kmuscrc/5A3C74053C73DB09/'+thefile, 'a')
                f.write(meslist[2])
                f.close() 
        else:
            print("i dont want this message")
            client1.publish(topiclist[3],"STOP:"+meslist[0])
            restartcheck[thefile] = True
            perform[thefile][1] += 1

    elif(len(meslist) == 2):
        if meslist[1] == "OK" : # stop 수신후 다시 보내는 거 ok
            client1.publish(topiclist[3],"RESTART:"+meslist[0]+":"+str(filenum[thefile]))
            perform[thefile][2] += 1
            # restartcheck[thefile] = True
            print("RESTART:"+meslist[0]+":"+str(filenum[thefile]))
            print("OK")

        elif meslist[1] == "DELETE_READY": # 다 보내고 삭제 대기하는 중
            print("DELETE_READY")
            if thefile not in senddone: # done을 보냈는지 체크
                senddone[thefile] = False
                print("NOT IN",senddone)
            if senddone[thefile]: # done 보냈으면
                client1.publish(topiclist[3],"DELETE:"+meslist[0])
                senddone[thefile] = True
                print("DELETE"+meslist[0])
            else: 
                client1.publish(topiclist[3],"RESTART:"+meslist[0]+":"+str(filenum[thefile]))
                perform[thefile][2] += 1
                print("RESTART:"+meslist[0]+":"+str(filenum[thefile]))
        else:
            print("other",message)
    else:
        if meslist[0] == "START_READY": # 1번부터 보내려고 준비
            client1.publish(topiclist[3],"START:")
            print("START_READY")
        else:
            print("other2",message)

# def on_disconnect(client, userdata, flags, rc=0): #브로커와 연결이 끊어졌을 때
#     global perform
#     global thefile
#     perform[thefile][4] = datetime.datetime.now()
#     with open("perform.txt",'a') as fw: #성능 딕셔너리 파일에 저장
#         for key,value in perform.items():
#             if key == thefile:
#                 # print(key)
#                 fw.write(f'{key} : {value}\n')

broker_address="10.10.10.1"
client1 = mqtt.Client("client1") # 구독자 이름
client1.on_connect = on_connect
client1.connect(broker_address) # 서버에 접속
# client1.on_disconnect=on_disconnect
# client1.subscribe("BIOLOGGER/#") #구독하는 토픽이름
# client1.on_message = on_message #callback 함수로 등록
client1.loop_forever() # 무한반복 

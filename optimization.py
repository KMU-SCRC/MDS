import paho.mqtt.client as mqtt
from datetime import datetime
import os
import atexit

def on_connect(client,userdata,flag,rc):
    """브로커에 연결되었을 때 실행되는 함수
    """
    print("Connect",str(rc))
    client1.subscribe("B/#") # 토픽 구독
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
        print ('Error: Creating directory. ',OSError +  directory)

perform = {} # 성능 체크하는 딕셔너리 // key[토픽/파일명] : value(받은 날짜,받은 시간, stop 보낸 횟수, restart 보낸 횟수, 재시작 횟수, 끝난 시각])
fileNum = {} # 받은 줄 수 관리 // key[토픽/파] : value(저장된 줄 수)
topic = ""
restartcheck = {} # 성능 체크시 재시작 횟수 체크를 위한 bool //key[토픽/파일명] : value( bool ) 
senddone = {} # DONE 보냈는지 BOOL로 관리 // key[토픽/파일명] : value( bool )
fileName = {} # save fileName //  topic : fileName
    

def savePerform(topic,perform):
    """save the perform
    Args:
        topic(str) : topic name
        perform(dict) : performance information
    """
    receivedTime = datetime.now()
    receivedTime = receivedTime.strftime('%H:%M:%S.%f')
    perform[topic][5] = receivedTime
    time_1 = datetime.strptime(perform[topic][1],'%H:%M:%S.%f')
    time_2 = datetime.strptime(receivedTime,'%H:%M:%S.%f')
    print(time_1,time_2)
    perform[topic][6] = time_2 -time_1
    with open("perform.txt",'a') as fw: #성능 딕셔너리 파일에 저장                    
        for key,value in perform.items():
            if key == topic:
                # print(key)
                fw.write(f'{key} : {value}\n')

def on_message(client,userdata,message):
    """메시지 왔을 때 실행되는 함수
    Args:
        client (int, hex()) : 받는 client 고유값
        userdata (int, hex()) : 보내는 client 고유값
        message (class) : 받는 message 값
    """
    global perform
    global topic
    global fileNum
    global fileName
    global senddone
    global restartcheck
    try:
        topic = message.topic # message.topic = BIOLOGGER/PENGUIN/1
        responTopic = topic + '/R'
        filePath = topic
        message = message.payload.decode("1250") # 1250 메시지 디코드
        print(message)
        mesList = message.split(':') # message = num : data, message = OK,DELETE_READY,DONE,START_READY
        if (len(mesList) == 2): # message form
            if mesList[0] == 'F': # the first
                filePath += '/'+mesList[1]
                if mesList[0] in fileName: # fileName is in the dict
                    print("Has filename in dict")
                    client1.publish(responTopic,"S:"+str(fileNum[topic]))
                else:
                    if os.path.isfile('/media/kmuscrc/237C250608A0AFEA/'+filePath): # file is already exist
                        print("파일이 존재")
                        senddone[topic] = False # 파일이 존재하고 딕셔너리에 없을 때
                        with open('/media/kmuscrc/237C250608A0AFEA/'+filePath) as myfile:
                            total_lines = sum(1 for line in myfile)
                        fileNum[topic] = total_lines + 1 # 원하는 건 다음번 줄
                        restartcheck[topic] = False
                        fileName[topic] = mesList[1]
                        receivedTime = datetime.now()
                        perform[topic] = [receivedTime.strftime('%Y/%m/%d'),receivedTime.strftime('%H:%M:%S.%f'),0,0,0,receivedTime.strftime('%H:%M:%S.%f'),receivedTime.strftime('%H:%M:%S.%f')]
                        print(perform)

                    else: # file is not exist
                        print("file is not exist")
                        createFolder('/media/kmuscrc/237C250608A0AFEA/'+topic)
                        fileNum[topic] = 1
                        senddone[topic] = False
                        receivedTime = datetime.now()
                        perform[topic] = [receivedTime.strftime('%Y/%m/%d'),receivedTime.strftime('%H:%M:%S.%f'),0,0,0,receivedTime.strftime('%H:%M:%S.%f'),receivedTime.strftime('%H:%M:%S.%f')]
                        restartcheck[topic] = False
                        fileName[topic] = mesList[1]
                    client1.publish(responTopic,"S:"+str(fileNum[topic])) # start message
                    print("start")

            elif mesList[0].isdigit():
                if int(mesList[0]) == (fileNum[topic]): # 행수가 같으면
                     # 파일에 저장 
                    fileNum[topic] += 1
                    f = open('/media/kmuscrc/237C250608A0AFEA/'+topic+'/'+fileName[topic], 'a')
                    f.write(mesList[1])
                    f.close()

                    if restartcheck[topic] : # 재시도 횟수 체크
                        print("restartcheck")
                        perform[topic][3] += 1
                        restartcheck[topic] = False
                
                else: # other message
                    print("DIFF",int(mesList[0]),fileNum[topic])
                    print(type(int(mesList[0])),type(fileNum[topic]))
                    print("i dont want this message")
                    client1.publish(responTopic,"T")
                    restartcheck[topic] = True
                    perform[topic][2] += 1

            elif mesList[0] == 'O': # stop 수신후 다시 보내는 거 ok
                client1.publish(responTopic,"S:"+str(fileNum[topic]))
                perform[topic][4] += 1
                print("RESTART:"+str(fileNum[topic]))

            elif  mesList[0] == 'N': # DONE 수신
                print("DONE")
                senddone[topic] = True
                client1.publish(responTopic,"D") # 파일 수신이 끝났으면 DELETE 메시지 발행
                print('send DELETE!')
                print("perform items",perform.items())
                savePerform(topic,perform)
                del fileNum[topic]
                del fileName[topic]
                del perform[topic]

            elif mesList[0] == "Y": # 다 보내고 삭제 대기하는 중
                print("DELETE_READY")
                if topic not in senddone: # done을 보냈는지 체크
                    senddone[topic] = False
                    print("NOT IN",senddone)
                    
                if senddone[topic]: # done 보냈으면
                    client1.publish(responTopic,"D:")
                    senddone[topic] = True
                    print("DELETE")
                else: 
                    client1.publish(responTopic,"S:"+str(fileNum[topic]))
                    perform[topic][4] += 1
                    print("RESTART:"+str(fileNum[topic]))
            else: # 메시지 형식에 맞지 않는 경우 
                print("other",message)
        else: # 파싱했을 때 길이가 2개가 아닌 경우 
            print("after parsing len is not 2",message)
    except Exception as e:
        print("on_message_err",e)


def handle_exit(topic,perform,fileNum):
    """when program is forced quit
    Args:
        topic(str) : topic name
        perform(dict): performance information
        fileNum(dict) : how much num received

    """
    savePerform(topic,perform)
    with open("perform.txt",'a') as fw:
        fw.write(fileNum[topic]+'\n')
    print("program is force quit")

atexit.register(handle_exit,topic,perform,fileNum)

# def on_disconnect(client, userdata, flags, rc=0): #브로커와 연결이 끊어졌을 때
#     global perform
#     global filePath
#     perform[thefile][4] = datetime.datetime.now()
#     with open("perform.txt",'a') as fw: #성능 딕셔너리 파일에 저장
#         for key,value in perform.items():
#             if key == thefile:
#                 # print(key)
#                 fw.write(f'{key} : {value}\n')

broker_address="192.168.50.195"
client1 = mqtt.Client("client1") # 구독자 이름
client1.on_connect = on_connect
client1.connect(broker_address) # 서버에 접속
# client1.on_disconnect=on_disconnect
client1.loop_forever() # 무한반복 
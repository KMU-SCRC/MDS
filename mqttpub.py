import paho.mqtt.client as mqtt
# from PIL import Image

# im=Image.open('pinguin_PNG.png')
# print(type(im))
# data = '''
# {
#   "employees": [
#     {
#       "name": "Surim",
#       "lastName": "Son"
#     },
#     {
#       "name": "Someone",
#       "lastName": "Huh"
#     },
#     {
#       "name": "Someone else",
#       "lastName": "Kim"
#     } 
#   ]
# }
# '''


mqttc = mqtt.Client("python_pub") 
mqttc.connect("192.168.50.240",1883)
# mqttc.publish("outTopic",im)
# f=open("sample.csv",'r')
# data=f.read()
# mqttc.publish("outTopic","send start")
# mqttc.publish("outTopic","1this is the fisrt")
# mqttc.publish("outTopic","2this is the second")
mqttc.publish("outTopic","the end")
# mqttc.publish("outTopic",data)
# print(data)
# f.close()
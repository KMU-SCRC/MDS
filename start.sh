#!/bin/sh
sudo kill $(sudo lsof -t -i:1883)
khadas
mosquitto &
cd /home/kmuscrc/Desktop/MDS
mkdir imin
python3 mqttnewsub.py 

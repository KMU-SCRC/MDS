#!/bin/sh
sudo iw phy phy0 interface add wlan1 type managed
sudo nmcli con up Hostspot
sudo kill $(sudo lsof -t -i:1883)
khadas
mosquitto &
cd /home/kmuscrc/Desktop/MDS
mkdir imin
python3 topicsubone.py 

#!/bin/bash
now=$(date +"%H:%M_%m_%d_%Y")
sudo mv /home/ubuntu/management-tools /home/ubuntu/management-tools_old_$now
sudo mkdir /home/ubuntu/management-tools
exit 0

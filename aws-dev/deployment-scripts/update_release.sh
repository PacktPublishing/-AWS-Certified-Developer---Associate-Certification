#!/bin/bash
now=$(date +"%H:%M_%m_%d_%Y")
EC2_INSTANCE_ID="`/usr/bin/wget -q -O - http://169.254.169.254/latest/meta-data/instance-id`"
EC2_ZONE="`/usr/bin/wget -q -O - http://169.254.169.254/latest/meta-data/placement/availability-zone`"
EC2_REGION="`echo $EC2_ZONE | sed -e 's:\([0-9][0-9]*\)[a-z]*\$:\\1:'`"

/bin/echo "deployTime:$now instanceid:$EC2_INSTANCE_ID zone:$EC2_ZONE region:$EC2_REGION application:$APPLICATION_NAME deploymentID:$DEPLOYMENT_ID" > /home/ubuntu/management-tools/release.txt 
exit 0

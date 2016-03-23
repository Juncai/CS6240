#!/bin/bash

EC2_RES=$(aws ec2 run-instances --image-id ami-8fcee4e5 --count 1 --instance-type t2.micro --key-name CS6240EC2KP --instance-initiated-shutdown-behavior terminate --security-groups default)

#!/bin/bash

# author: Xi Wang
export AWS_ACCESS_KEY_ID=AKIAIPJFSKFCPSVYBDDA
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=us-east-1

# please create a key pair
export EC2_KEY_PAIR_NAME=test_key
export EC2_PRIVATE_KEY_PATH=/home/alexwang/key/test_key.pem

# security group information
export EC2_SECURITY_GROUP=default

# AMI
export IMAGE_ID=ami-8fcee4e5

# ec2 username
export EC2_USERNAME=ec2-user

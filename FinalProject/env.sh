#!/bin/bash

# author: Xi Wang
export AWS_ACCESS_KEY_ID=AKIAJSXCXRGQET7DDGMQ
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=us-east-1

# please create a key pair
export EC2_KEY_PAIR_NAME=alexwang_ec2
export EC2_PRIVATE_KEY_PATH=/home/alexwang/key/alexwang_ec2.pem

# security group information
export EC2_SECURITY_GROUP=default

# AMI
export IMAGE_ID=ami-8fcee4e5

# ec2 username
export EC2_USERNAME=ec2-user

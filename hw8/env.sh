#!/bin/bash

# author: Xi Wang
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_DEFAULT_REGION=us-east-1

# please create a key pair
export EC2_KEY_PAIR_NAME=Emma
export EC2_PRIVATE_KEY_PATH=

# security group information
export EC2_SECURITY_GROUP=A8SecurityGroup

# s3 bucket name
export INPUT_BUCKET=cs6240sp16
export OUTPUT_BUCKET=juncai001

# AMI
export IMAGE_ID=ami-8fcee4e5

# ec2 username
export EC2_USERNAME=ec2-user
# In ReadMe: please make sure install node and json
# https://github.com/trentm/json

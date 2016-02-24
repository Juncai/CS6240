#!/bin/bash
# authors: Rohan Joshi, Vedant Naik
# this file will contain all the paths that you need to plug into the job to get the s3 bucket to run
# please plug your paths as indicated by the comments
# if you need more information, please refer the readme

#downloadpath="/Users/rohanjoshi/Documents/output_aws_lin"
#bucketname="linearreg"
# LEAVE THE downloadpath EMPTY if the path is relative, else completely give the path with the leading '/' 

#echo $bucketname

#!/bin/bash
bucketname=''
downloadpath=''

if [ -f script.cfg ];then 
	. script.cfg
fi

# for checking the correct path and bucketname:
#echo $bucketname $downloadpath
# now starting the script that runs the job on AWS, waits for completion and then downloads the files
# for processing in R

# listing the contents in the bucket to see if the bucket name is correct
# this command will also tell you if the CLI is configured correctly
aws s3 ls s3://$bucketname
sh run_aws.sh $downloadpath $bucketname
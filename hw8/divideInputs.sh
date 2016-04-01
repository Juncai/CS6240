#!/bin/bash
#Xinyuan Wang
####################Constants#################
INPUT_PATH=$1
NODE_NUM=$2
total_size=0
####################Functions#################
function split() {
  local acc_size=0
  for index in ${!name_array[@]}
  do
    local i=1
    acc_size=$(( acc_size+size_array[index] ))  
    while [ "$i" -le "$NODE_NUM" ]
    do
      local j=$[$i-1]
      if [ "$acc_size" -le $(( chunk_size*i )) ] && [ "$acc_size" -gt $(( chunk_size*j )) ]
      then
        echo "climate/${name_array[index]}" >> ./inputs/input_$j
      fi
      i=$[$i+1]
    done
  done
}
####################Main######################
if [ -z $INPUT_PATH ] || [ -z $NODE_NUM ]
then
  echo "Usage: ./divideInputs.sh S3_INPUT_PATH NUM_OF_NODE"
  exit 1
fi
sizes=$(aws s3 ls $INPUT_PATH | awk 'NR!=1{printf $3 ":"}')
fileNames=$(aws s3 ls $INPUT_PATH | awk 'NR!=1{printf $4 ":"}')

IFS=':' read -a size_array <<< "${sizes}"
IFS=':' read -a name_array <<< "${fileNames}"
for elem in ${size_array[@]}
do
  total_size=$(( elem+total_size ))
done
chunk_size=$(( total_size / NODE_NUM ))
split 

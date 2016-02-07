#!/bin/bash

strindex() { 
  x="${1%%$2*}"
  [[ $x = $1 ]] && echo -1 || echo ${#x}
}

emr() { 
  path="$(find . -name "cid.tmp")"
  cid=$(cat "$path")

  readydatetimetext=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{16\}"ReadyDateTime": [0-9][0-9]*')
  enddatetimetext=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{16\}"EndDateTime": [0-9][0-9]*')
  
  delimiter=":"
  readydatelengthstart=$(strindex "$readydatetimetext" "$delimiter")
  enddatetimelengthstart=$(strindex "$enddatetimetext" "$delimiter")
  
  readydatelengthfin=${#readydatetimetext}
  enddatetimelengthfin=${#enddatetimetext}
  
  readydatetime=${readydatetimetext:$((readydatelengthstart+2)):$((readydatelengthfin))}
  enddatetime=${enddatetimetext:$((enddatetimelengthstart+2)):$((enddatetimelengthfin))}
  
  elapsedtime=$((enddatetime-readydatetime))
  echo "$elapsedtime"
}

size=$2
pre=$(date +%s)

./$1

post=$(date +%s)

realname=$1
desiredlength=${#realname}
outputname=${realname:0:desiredlength}
delimiter="_"
offset=$(strindex "$outputname" "$delimiter")
executiontype=${outputname:6:offset}
interprogramtype=${outputname:$((offset+1)):desiredlength}
programtype=$interprogramtype$size

emr="EMR"
containsemr=$(strindex $executiontype "$emr")

if [ $containsemr -eq -1 ]
then
  actualtime=$((post-pre))
else
  actualtime=$(emr)
fi

echo "${executiontype}, ${actualtime}, ${programtype}" >> my_file.csv

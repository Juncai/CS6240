
#!/bin/bash

strindex() { 
  x="${1%%$2*}"
  [[ $x = $1 ]] && echo -1 || echo ${#x}
}

emr() { 
  path="$(find . -name "cid.temp")"
  cid=$(cat "$path")

  readydatetimetext=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{4\}"ReadyDateTime": ')
  enddatetimetext=$(aws emr describe-cluster --cluster-id "$cid" | grep -oh '^[[:space:]]\{6\}"EndDateTime": ')
  
  delimiter=":"
  readydatelengthstart=$(strindex "$readydatetimetext" "$delimiter")
  enddatetimelengthstart=$(strindex "$enddatetimetext" "$delimiter")
  
  readydatelengthfin=${#readydatetimetext}
  enddatetimelengthfin=${#enddatetimetext}
  
  readydatetime=${readydatetimetext:$((readydatelengthstart+2)):$((readydatelengthfin-1))}
  enddatetimetext=${readydatetimetext:$((enddatetimelengthstart+2)):enddatetimelength}
  
  elapsedtime=$((elapseddatetimes-readydatetime))
  echo "$elapsedtime"
}

size=$2
pre=$(date +%s)

./$1

post=$(date +%s)

realname=$1
desiredlength=${#realname}
desiredlength=$((desiredlength-3))
outputname=${realname:0:desiredlength}
delimiter="_"
offset=$(strindex "$outputname" "$delimiter")
executiontype=${outputname:9:offset}
interprogramtype=${outputname:$((offset+1)):desiredlength}
programtype=$interprogramtype$size

emr="emr"
containsemr=$(strindex $executiontype "$emr")

if [ $containsemr -eq -1 ]
then
  actualtime=$((post-pre))
else
  actualtime=$(emr)
fi

echo "${executiontype}, ${actualtime}, ${programtype}" >> my_file.csv


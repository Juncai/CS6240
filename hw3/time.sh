
#!/bin/bash

strindex() { 
  x="${1%%$2*}"
  [[ $x = $1 ]] && echo -1 || echo ${#x}
}

size=$2
pre=$(date +%s)

./$1

post=$(date +%s)
actual=$((post-pre))

realname=$1
desiredlength=${#realname}
desiredlength=$((desiredlength-3))
outputname=${realname:0:desiredlength}
delimiter="_"
offset=$(strindex "$outputname" "$delimiter")
executiontype=${outputname:9:offset}
interprogramtype=${outputname:$((offset+1)):desiredlength}
programtype=$programtype$size

echo "${executiontype}, ${actual}, ${programtype}" >> my_file.csv


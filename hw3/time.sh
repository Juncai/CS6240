
#!/bin/bash

strindex() { 
  x="${1%%$2*}"
  [[ $x = $1 ]] && echo -1 || echo ${#x}
}

pre=$(date +%s)

bash $1

post=$(date +%s)
actual=$((post-pre))

realname=$1
desiredlength=${#realname}
desiredlength=$((desiredlength-3))
outputname=${realname:0:desiredlength}
delimiter="_"
offset=$(strindex "$outputname" "$delimiter")
executiontype=${outputname:9:offset}
programtype=${outputname:$((offset+1)):desiredlength}

echo "${executiontype}, ${actual}, ${programtype}" >> my_file.csv


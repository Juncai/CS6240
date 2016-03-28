ips[0]=1
i="0"

while [ $i -lt 5 ]
do
	ips[$i]=$i

	i=$[$i+1]
done

for ip in "${ips[@]}"
do
	echo $ip
done

i="0"
while IFS='' read -r line || [[ -n "$line" ]]; do
	ips[$i]=$line
	i=$[$i+1]
done < "address.txt"

ssh -i $EC2_PRIVATE_KEY_PATH ec2-user@${ips[$1]}

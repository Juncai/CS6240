#!/bin/bash

# TOTAL=$(du -hs /Users/Emma/Desktop/A8/data/ | tail -1)   # 1.9G
# TOTAL_IN_BYTES=2040109465.6

# get from loop
# TOTAL_IN_BYTES=2011023651

INSTANCE_NUM=8
HALF_IN_BYTES=1005511826
EIGHTH_IN_BYTES=251377956

FILESIZE=0

if ((INSTANCE_NUM == 2 )); then
	echo "inside 2 block"
	for FILENAME in /Users/Emma/Desktop/A8/data/*.gz; do
	    FILESIZE=$(($FILESIZE + $(stat -f "%z" "$FILENAME")))
	    if(( $FILESIZE < $HALF_IN_BYTES )); then
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/twoInstanceList1.txt
	    else
	    	# need to split file name and make s3 file path
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/twoInstanceList2.txt
	    fi
	done
else if ((INSTANCE_NUM == 8 )); then
	for FILENAME in /Users/Emma/Desktop/A8/data/*.gz; do
	    FILESIZE=$(($FILESIZE + $(stat -f "%z" "$FILENAME")))
	    if(( $FILESIZE < $EIGHTH_IN_BYTES )); then
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/eightInstanceList1.txt
	    elif (( $FILESIZE < $(($EIGHTH_IN_BYTES * 2 )))); then
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/eightInstanceList2.txt
	    elif (( $FILESIZE < $(($EIGHTH_IN_BYTES * 3 )))); then
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/eightInstanceList3.txt
	    elif (( $FILESIZE < $(($EIGHTH_IN_BYTES * 4 )))); then
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/eightInstanceList4.txt
	    elif (( $FILESIZE < $(($EIGHTH_IN_BYTES * 5 )))); then
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/eightInstanceList5.txt
	    elif (( $FILESIZE < $(($EIGHTH_IN_BYTES * 6 )))); then
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/eightInstanceList6.txt
	    elif (( $FILESIZE < $(($EIGHTH_IN_BYTES * 7 )))); then
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/eightInstanceList7.txt
	    else 
	    	echo $FILENAME >> /Users/Emma/Desktop/A8/eightInstanceList8.txt
	    fi  
	done
fi
fi


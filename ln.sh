#!/bin/bash
date=`date|tr " " "_"`
for file in *
do
	if [ -d $file ]
	then
		continue
	elif [[ "$file" =~ ^ln.sh~?$ ]]
	then
		continue
	elif [ -e ../$file ]
	then
		if [ ! -e ../backup$$_$date ]
		then
			mkdir ../"backup$$_$date"
		fi		
		cp ../$file ../backup$$_$date/$file
		rm ../$file
	fi
	ln $file ../
done

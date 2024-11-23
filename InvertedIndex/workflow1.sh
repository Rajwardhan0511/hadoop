#!/bin/bash

# author: Rajwardhan 
# Created: 9th Nov 2024
# Last Modified: 11th Nov 2024

# Description:
# Implementing the automated script for extracting inverted index 

# Usage:
# bash workflow1.sh 
# OR if require more than one reducer then
# bash workflow1.sh <no. of reducers>  


#Checking if the folder is available on hdfs , If yes then removing 
hdfs dfs -test -d inverted &> /dev/null
if [[ $? -eq 0 ]];then
	hdfs dfs -rm -r inverted > /dev/null
fi


#Storing present working directory path in variable
path=$(pwd)



#Creating folder on hdfs and putting files in that folder 
hdfs dfs -mkdir inverted
hdfs dfs -put hortonworks.txt inverted/


#Going in workspace and checking if project named InvertedIndex exists , If yes then removing 
cd  /home/cloudera/workspace/
find InvertedIndex &> /dev/null
if [[ $? -eq 0 ]];then
	rm -rf InvertedIndex
fi

#Creating project 
mkdir -p InvertedIndex/src/inverted
mkdir -p InvertedIndex/bin/inverted
cd InvertedIndex
invPath=$(pwd)

#Copying java file in project
cd src/inverted/
cp "$path/IndexInverterJob.java" .

lib="/usr/lib/hadoop/client/*"
#CLASSPATH=$(find "$lib" -name "*.jar" | tr '\n' ':')
javac -d "$invPath/bin" -classpath "$lib" "$invPath/src/inverted/IndexInverterJob.java"

cd "$invPath/bin/"
jar -cf "$path/InvertedIndex.jar" ./inverted

cd "$path"

jar -tf InvertedIndex.jar

r=1
if [[ $# -gt 0 ]];then
	r=$1
fi

yarn jar InvertedIndex.jar inverted.IndexInverterJob -D mapred.reduce.tasks=$r inverted/hortonworks.txt inverted/output

hdfs dfs -ls inverted/output

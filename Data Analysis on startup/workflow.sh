#!/bin/bash
# Author: Rajwardhan and Darshan
# Created: 23-11-2024
# Last modified: 25-11-2024

# Description:
# Automation script Analysis of Startups

# Usage: 
# bash workflow.sh 


echo "Starting workflow..."

# Run the DDL Hive script
hive -f ddl.hive 2> /dev/null
if [ $? -ne 0 ];then
    echo "Error: Failed to execute 'ddl.hive'."
    exit 1
fi

# Run the DML Hive script
hive -f dml.hive 2> /dev/null
if [ $? -ne 0 ];then
    echo "Error: Failed to execute 'dml.hive'."
    exit 1
fi

echo "Analysis completed successfully!"

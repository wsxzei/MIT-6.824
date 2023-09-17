#!/bin/bash

cd ../..
for ((i=1; i<=100; i++))
do
    echo "Running command: $i"
    go test -run 2A
done

#!/bin/bash

cd ../..
  echo "" > "./debug/raft_2A_2.log"
for ((i=1; i<=150; i++))
do
  echo -e "\n\n" >> "./debug/raft_2A_2.log"
    echo "Running command: $i" >> "./debug/raft_2A_2.log"
    go test -run 2A >> "./debug/raft_2A_2.log"
done

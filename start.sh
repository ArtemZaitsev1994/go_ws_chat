#!/bin/bash
/wait
ls
pwd
apt-get install git
go get -d -v ./...
go build
./backend_chat
ls

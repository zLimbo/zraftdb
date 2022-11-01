#!/bin/bash

cd zpbft
CGO_ENABLED=0  GOOS=linux  GOARCH=amd64
go build -race -o ../bin/ .

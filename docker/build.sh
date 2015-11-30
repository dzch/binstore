#!/bin/bash

script_dir=$(dirname $0)
cur_dir=$(pwd)
cd $script_dir
script_dir=$(pwd)
cd $scriptd_dir/..
go build binstore_bin.go && cp binstore_bin docker
cd $script_dir

sudo docker build -f Dockerfile -t binstore .
cd $cur_dir

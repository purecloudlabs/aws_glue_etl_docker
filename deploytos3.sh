#!/usr/bin/env bash
rm -rf aws_glue_etl_docker_deploy
mkdir aws_glue_etl_docker_deploy
cd aws_glue_etl_docker_deploy
echo "git+https://github.com/purecloudlabs/aws_glue_etl_docker" > requirements.txt
mkdir deps
virtualenv -p python2.7 .
bin/pip2.7 install -r requirements.txt --install-option --install-lib="$(PWD)/deps"
cd deps && zip -r ../aws_glue_etl_docker_deploy.zip . && cd ..
aws s3 cp ./aws_glue_etl_docker_deploy.zip $1
  

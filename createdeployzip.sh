!/usr/bin/env bash
rm -rf aws_glue_etl_docker_deploy
mkdir aws_glue_etl_docker_deploy
cd aws_glue_etl_docker_deploy
mkdir deps
virtualenv -p python2.7 .
pip install -t deps git+https://github.com/purecloudlabs/aws_glue_etl_docker
cd deps && zip -r ../aws_glue_etl_docker_deploy.zip . && cd ..
aws s3 cp ./aws_glue_etl_docker_deploy.zip $1
 
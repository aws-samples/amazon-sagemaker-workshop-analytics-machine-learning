#!/bin/bash

if ! command -v aws &> /dev/null
then
    echo "Please install the AWS CLI"
    exit
fi

while getopts p:* flag
do
    case "${flag}" in
        p) pipeline=${OPTARG};;
    esac
done

account_id=$(aws sts get-caller-identity | jq '.Account' -r)
region=$(aws configure get region)

aws s3 mb s3://sagemaker-servicecatalog-seedcode-"$account_id"-"$region"
aws s3 cp cloud_formation/jenkins-git-template-custom-v1.yml s3://sagemaker-servicecatalog-seedcode-"$account_id"-"$region"/jenkins-git-template-custom-v1.yml

aws s3 cp GitRepositorySeedCodeCheckinCodeBuildProject-v1.0.zip s3://sagemaker-servicecatalog-seedcode-"$account_id"-"$region"/bootstrap/GitRepositorySeedCodeCheckinCodeBuildProject-v1.0.zip

for i in $(ls -d *seedcode*); do
    cd $i && zip -r ../$i.zip . && cd ..
    aws s3 cp $i.zip s3://sagemaker-servicecatalog-seedcode-"$account_id"-"$region"/toolchain/$i.zip
    rm $i.zip
done
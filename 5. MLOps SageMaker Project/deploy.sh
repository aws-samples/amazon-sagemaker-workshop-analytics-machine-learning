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

aws s3 mb s3://cloud-formation-"$account_id"-"$region"
aws s3 cp cloud_formation/jenkins-git-template-custom-v1.yml s3://cloud-formation-"$account_id"-"$region"/jenkins-git-template-custom-v1.yml

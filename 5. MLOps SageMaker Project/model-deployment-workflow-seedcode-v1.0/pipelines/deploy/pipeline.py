"""Example workflow pipeline script for NYCTaxi deploy pipeline.

    createmodel -> batch transform

Implements a get_pipeline(**kwargs) method.
"""
import os

import boto3
import sagemaker
import sagemaker.session

import json
import logging
import os

from sagemaker.inputs import CreateModelInput
from sagemaker.workflow.steps import CreateModelStep
from sagemaker.model import Model

from sagemaker.workflow.parameters import (
    ParameterInteger,
    ParameterString,
)
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.transformer import Transformer
from sagemaker.inputs import TransformInput
from sagemaker.workflow.steps import TransformStep

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

import sys
sys.path.append(BASE_DIR)

from model_registry import ModelRegistry
from batch_config import BatchConfig


registry = ModelRegistry()

def get_sagemaker_client(region):
    """Gets the sagemaker client.

        Args:
            region: the aws region to start the session
            default_bucket: the bucket to use for storing the artifacts

        Returns:
            `sagemaker.session.Session instance
    """
    boto_session = boto3.Session(region_name=region)
    sagemaker_client = boto_session.client("sagemaker")
    return sagemaker_client


def get_session(region, default_bucket):
    """Gets the sagemaker session based on the region.

    Args:
        region: the aws region to start the session
        default_bucket: the bucket to use for storing the artifacts

    Returns:
        `sagemaker.session.Session instance
    """

    boto_session = boto3.Session(region_name=region)

    sagemaker_client = boto_session.client("sagemaker")
    runtime_client = boto_session.client("sagemaker-runtime")
    return sagemaker.session.Session(
        boto_session=boto_session,
        sagemaker_client=sagemaker_client,
        sagemaker_runtime_client=runtime_client,
        default_bucket=default_bucket,
    )

# def get_pipeline_custom_tags(new_tags, region, sagemaker_project_arn=None):
#     try:
#         sm_client = get_sagemaker_client(region)
#         response = sm_client.list_tags(
#             ResourceArn=sagemaker_project_arn)
#         project_tags = response["Tags"]
#         for project_tag in project_tags:
#             new_tags.append(project_tag)
#     except Exception as e:
#         print(f"Error getting project tags: {e}")
#     return new_tags


def get_pipeline(
    region,
    sagemaker_project_arn=None,
    role=None,
    default_bucket=None,
    model_package_group_name="DYCTaxiPackageGroup",
    pipeline_name="DYCTrainPipeline",
    base_job_prefix="DYCTaxiTrain",
):
    """Gets a SageMaker ML Pipeline instance working with on abalone data.

    Args:
        region: AWS region to create and run the pipeline.
        role: IAM role to create and run steps and pipeline.
        default_bucket: the bucket to use for storing the artifacts

    Returns:
        an instance of a pipeline
    """
    sagemaker_session = get_session(region, default_bucket)
    if role is None:
        role = sagemaker.session.get_execution_role(sagemaker_session)

   
    
    # Get the stage specific deployment config for sagemaker
    with open("pipelines/deploy/batch-config.json", "r") as f:
        j = json.load(f)
        batch_config = BatchConfig(**j)
        
    # If we don't have a specific champion variant defined, get the latest approved
    if batch_config.model_package_version is None:
        print("Selecting latest approved")
        p = registry.get_latest_approved_packages(model_package_group_name, max_results=1)[0]
        batch_config.model_package_version = p["ModelPackageVersion"]
        batch_config.model_package_arn = p["ModelPackageArn"]
    else:
        # Get the versioned package and update ARN
        print(f"Selecting variant version {batch_config.model_package_version}")
        p = registry.get_versioned_approved_packages(
            package_group_name,
            model_package_versions=[batch_config.model_package_version],
        )[0]
        batch_config.model_package_arn = p["ModelPackageArn"]
        
    # Set the default input data uri
    data_uri = f"s3://{default_bucket}/{base_job_prefix}/input/test/"

    # set the output transform uri
    transform_uri = f"s3://{default_bucket}/{base_job_prefix}/transform"
    
    # Get the pipeline execution to get the baseline uri
    pipeline_execution_arn = registry.get_pipeline_execution_arn(
        batch_config.model_package_arn
    )
    print(f"Got pipeline exection arn: {pipeline_execution_arn}")
    model_uri, image_uri = registry.get_model_artifact(pipeline_execution_arn)
    print(f"Got model uri: {model_uri}")
    
    # parameters for pipeline execution
    input_data_uri = ParameterString(
        name="DataInputUri",
        default_value=data_uri,
    )
    input_model_uri = ParameterString(
        name="ModelInputUri",
        default_value=model_uri,
    )
    output_transform_uri = ParameterString(
        name="TransformOutputUri",
        default_value=transform_uri,
    )
    transform_instance_count = ParameterInteger(
        name="TransformInstanceCount", default_value=1
    )
    transform_instance_type = ParameterString(
        name="TransformInstanceType", default_value="ml.m5.xlarge"
    )
        
    model = Model(
        image_uri=image_uri,
        model_data=input_model_uri,
        sagemaker_session=sagemaker_session,
        role=role,
    )
    

    inputs = CreateModelInput(
        instance_type="ml.m5.xlarge",
    )
    step_create_model = CreateModelStep(
        name="TripFareCreateModel",
        model=model,
        inputs=inputs,
    )
    
    
    transformer = Transformer(
        model_name=step_create_model.properties.ModelName,
        instance_type=transform_instance_type,
        instance_count=transform_instance_count,
        output_path=output_transform_uri,
        accept="text/csv",
        assemble_with="Line"
    )
    
    step_transform = TransformStep(
        name="TripFareTransform",
        transformer=transformer,
        inputs=TransformInput(
            data=input_data_uri,
            content_type="text/csv", 
            split_type="Line",
            input_filter="$[1:]",
            join_source="Input",
            ),
    )

    # pipeline instance
    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            input_data_uri,
            input_model_uri,
            output_transform_uri,
            transform_instance_count,
            transform_instance_type,
        ],
        steps=[step_create_model, step_transform],
        sagemaker_session=sagemaker_session,
    )
    return pipeline

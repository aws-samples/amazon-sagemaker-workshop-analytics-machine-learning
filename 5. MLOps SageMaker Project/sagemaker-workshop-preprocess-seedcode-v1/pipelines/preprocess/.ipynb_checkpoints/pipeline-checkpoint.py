"""Example workflow pipeline script for preprocess pipeline.

Implements a get_pipeline(**kwargs) method.
"""
import os

import boto3
import sagemaker
import sagemaker.session


from sagemaker.processing import (
    ProcessingInput,
    ProcessingOutput,
    ScriptProcessor,
)
from sagemaker.sklearn.processing import SKLearnProcessor

from sagemaker.workflow.parameters import (
    ParameterInteger,
    ParameterString,
)
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.steps import (
    ProcessingStep,
)


BASE_DIR = os.path.dirname(os.path.realpath(__file__))

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
    pipeline_name="preprocess",
    base_job_prefix="NYCTaxipreprocess",
):
    """Gets a SageMaker ML Pipeline instance working with on NYC Taxi data.

    Args:
        region: AWS region to create and run the pipeline.
        role: IAM role to create and run steps and pipeline.
        default_bucket: the bucket to use for storing the artifacts

    Returns:
        an instance of a pipeline
    """
    sagemaker_session = get_session(region, default_bucket)
    
    account_id = boto3.client("sts", region_name=region).get_caller_identity()["Account"]
    
    if role is None:
        role = sagemaker.session.get_execution_role(sagemaker_session)

    # parameters for pipeline execution
    processing_instance_count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)
    processing_instance_type = ParameterString(
        name="ProcessingInstanceType", default_value="ml.m5.xlarge"
    )
    input_data = ParameterString(
        name="InputDataUrl",
        default_value=f"s3://sagemaker-{region}-{account_id}/sagemaker/DEMO-xgboost-tripfare/input/data/green/",
    )
    input_zones = ParameterString(
        name="InputZonesUrl",
        default_value = f"s3://sagemaker-{region}-{account_id}/sagemaker/DEMO-xgboost-tripfare/input/zones/taxi_zones.zip",
    )

    # processing step for feature engineering
    sklearn_processor = SKLearnProcessor(
        framework_version="0.23-1",
        instance_type=processing_instance_type,
        instance_count=processing_instance_count,
        base_job_name=f"{base_job_prefix}/xgboost-tripfare-preprocess",
        sagemaker_session=sagemaker_session,
        role=role,
    )
    step_process = ProcessingStep(
        name="PreprocessNYCTaxiData",
        processor=sklearn_processor,
        inputs=[
            ProcessingInput(
                source=input_data,
                destination="/opt/ml/processing/input/data",
                s3_data_distribution_type="ShardedByS3Key",
            ),
            ProcessingInput(
                source=input_zones,
                destination="/opt/ml/processing/input/zones",
                s3_data_distribution_type="FullyReplicated",
            ),
        ],
        outputs=[
            ProcessingOutput(output_name="train", 
                             source="/opt/ml/processing/train", 
                             destination=f"s3://{default_bucket}/{base_job_prefix}/input/train/"
                            ),
            ProcessingOutput(output_name="validation", 
                             source="/opt/ml/processing/validation",
                             destination=f"s3://{default_bucket}/{base_job_prefix}/input/validation/"
                            ),
            ProcessingOutput(output_name="test", 
                             source="/opt/ml/processing/test",
                             destination=f"s3://{default_bucket}/{base_job_prefix}/input/test/"
                            ),
        ],
        code=os.path.join(BASE_DIR, "preprocess.py"),
#         job_arguments=["--input-data", input_data],
    )

    # pipeline instance
    pipeline = Pipeline(
        name=pipeline_name,
        parameters=[
            processing_instance_type,
            processing_instance_count,
            input_data,
            input_zones
        ],
        steps=[step_process],
        sagemaker_session=sagemaker_session,
    )
    return pipeline

"""Evaluation script for measuring mean squared error."""
import json
import logging
import pathlib
import pickle
import tarfile

import numpy as np
import pandas as pd
import xgboost
import glob

from math import sqrt
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

if __name__ == "__main__":
    logger.debug("Starting evaluation.")
    
    import os
    for file in os.listdir("/opt/ml/processing/model"):
        logger.info(file)
        
    model_path = "/opt/ml/processing/model/model.tar.gz"
    with tarfile.open(model_path) as tar:
        def is_within_directory(directory, target):
            
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)
        
            prefix = os.path.commonprefix([abs_directory, abs_target])
            
            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise Exception("Attempted Path Traversal in Tar File")
        
            tar.extractall(path, members, numeric_owner=numeric_owner) 
            
        
        safe_extract(tar, path=".")
    
    for file in os.listdir("/opt/ml/processing/model"):
        logger.info(file)
        
    logger.debug("Loading xgboost model.")
    model = pickle.load(open("xgboost-model", "rb"))

    logger.debug("Reading test data.")
    test_path = "/opt/ml/processing/test/"
    all_files = glob.glob(test_path + "/*.csv")
    tmp = []
    for filename in all_files:
        df_tmp = pd.read_csv(filename, index_col=None, header=None)
        tmp.append(df_tmp)

    df = pd.concat(tmp, axis=0, ignore_index=True)

    logger.debug("Reading test data.")
    y_test = df.iloc[:, 0].values
    df.drop(df.columns[0], axis=1, inplace=True)
    X_test = xgboost.DMatrix(df.values)

    logger.info("Performing predictions against test data.")
    predictions = model.predict(X_test)

    # See the regression metrics
    # see: https://docs.aws.amazon.com/sagemaker/latest/dg/model-monitor-model-quality-metrics.html
    logger.debug("Calculating metrics.")
    mae = mean_absolute_error(y_test, predictions)
    mse = mean_squared_error(y_test, predictions)
    rmse = sqrt(mse)
    r2 = r2_score(y_test, predictions)
    std = np.std(y_test - predictions)
    report_dict = {
        "regression_metrics": {
            "mae": {
                "value": mae,
                "standard_deviation": std,
            },
            "mse": {
                "value": mse,
                "standard_deviation": std,
            },
            "rmse": {
                "value": rmse,
                "standard_deviation": std,
            },
            "r2": {
                "value": r2,
                "standard_deviation": std,
            },
        },
    }

    output_dir = "/opt/ml/processing/evaluation"
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    logger.info("Writing out evaluation report with rmse: %f", rmse)
    evaluation_path = f"{output_dir}/evaluation.json"
    with open(evaluation_path, "w") as f:
        f.write(json.dumps(report_dict))
import json
import os
import sys

from gbq import BigQuery
from pora_bigquery import PoraBigquery
from pipeline_exceptions import DatasetSchemaDirectoryNonExistent, DeployFailed

sys.tracebacklimit = 0

def _validate_env_variables():
    if not os.environ.get("gcp_project"):
        raise Exception("Missing `gcp_project` config")

    if not os.environ.get("dataset_schema_directory"):
        raise Exception("Missing `dataset_schema_directory` config")

    if not os.environ.get("credentials"):
        raise Exception("Missing `credentials` config")


def _validate_if_path_exists():
    dataset_schema_directory = os.environ.get("dataset_schema_directory")
    print(f"Directory path to file {dataset_schema_directory}")
    return os.path.isdir(dataset_schema_directory)


def _csv_import():
    deploy_failed = False
    print(' Find variables -->')
    dataset_schema_directory = os.environ.get("dataset_schema_directory")
    credentials = os.environ.get("credentials")
    gcp_project = os.environ.get("gcp_project")

    try:
        print (f"Deploying crendential {credentials} and project {gcp_project}")
        # Service account email address as listed in the Google Developers Console.
        svc_account = credentials
       
        #bq = BigQuery(svc_account=svc_account, project = gcp_project)

        bq = PoraBigquery(svc_account=svc_account, project = gcp_project)

        #bq = BigQuery(credentials, gcp_project)
        print (f"Open BigQuery objets ==> {dataset_schema_directory}")
        for root, dirs, files in os.walk(dataset_schema_directory):
            print(f"Split root directory {root} {dirs} ")
            dataset = root.split("/").pop()
            for file in files:
               #def bigquery_import_csv(self, file_path: str, table_id: str):
               if os.path.is_directory(root + "/" + file) and file == TABLE_NAME:
                 root_in_table, dirs_in_table, files_in_table = os.walk(dataset_schema_directory + "/" + file) 
                 for file_in_table in files_in_table:
                    #bq.bigquery_import_csv( root + "/" + file, file)
                    print(f"Imported {file_in_table} to {dataset} in Bigquery")

                    
    except Exception as e:
        print(f"Failed to execute in Bigquery: {e}")
        deploy_failed = True

    if deploy_failed:
        raise DeployFailed


_validate_env_variables()

print("-------EXECUTE----------")

if _validate_if_path_exists():
    print("------EXECUTE-----------")
    _csv_import()
    print("------|||||||||-----------")
else:
    raise DatasetSchemaDirectoryNonExistent
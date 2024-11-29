import json
import os
import sys

from gbq import BigQuery

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


def _execute():
    deploy_failed = False
    print(' Find variables -->')
    dataset_schema_directory = os.environ.get("dataset_schema_directory")
    credentials = os.environ.get("credentials")
    gcp_project = os.environ.get("gcp_project")

    try:
        print (f"Deploying crendential {credentials} and project {gcp_project}")
        # Service account email address as listed in the Google Developers Console.
        svc_account = credentials
       
        bq = BigQuery(svc_account=svc_account, project = gcp_project)

        #bq = BigQuery(credentials, gcp_project)
        print (f"Open BigQuery objets ==> {dataset_schema_directory}")
        for root, dirs, files in os.walk(dataset_schema_directory):
            print(f"Split root directory {root} {dirs} ")
            dataset = root.split("/").pop()
            for file in files:
                with open(f"{root}/{file}", "r") as contents:
                    file_name_and_extension = file.split(".")
                    print(
                        f"Execute Query schema SELECT * FROM {gcp_project}.{dataset}.{file_name_and_extension[0]}"
                    )

                     bq.execute(
                            f"SELECT * FROM {gcp_project}.{dataset}.{file_name_and_extension[0]}"
                        )
                    
    except Exception as e:
        print(f"Failed to execute in Bigquery: {e}")
        deploy_failed = True

    if deploy_failed:
        raise DeployFailed


_validate_env_variables()

print("-------EXECUTE----------")

if _validate_if_path_exists():
    print("------EXECUTE-----------")
    _execute()
    print("------|||||||||-----------")
else:
    raise DatasetSchemaDirectoryNonExistent
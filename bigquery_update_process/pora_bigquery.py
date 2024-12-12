from typing import Dict, List, Optional, Union
import json
from google.cloud import bigquery
from google.cloud.bigquery import QueryJob
from google.cloud.bigquery.dataset import DatasetListItem
from google.cloud.bigquery.routine import Routine, RoutineArgument
from google.api_core.exceptions import NotFound, ClientError
from google.cloud.bigquery.table import PartitionRange, Table
from gbq import BigQuery

class PoraBigquery(BigQuery):
    def __init__(self, svc_account: str, project: Optional[str] = None):
        super().__init__(svc_account, project)

    def bigquery_import_csv(self, file_path: str, json_file_path: str, table_id: str, schema_id: str):
        
        schema_to_bigquery = []
        
        with open(json_file_path) as f:
            json_data = json.load(f)
                    
            for key, value in json_data.items():
                schema_to_bigquery.append( bigquery.SchemaField(key,value))    
                
                #print(f" in Dict Json value: {key} and {value}")

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format = bigquery.SourceFormat.CSV,
                field_delimiter = ",",
                skip_leading_rows = 1,
                schema = schema_to_bigquery
            )

            with open(file_path, "rb") as source_file:
                job = self.bq_client.load_table_from_file(
                        source_file,
                        table_id,
                        job_config = job_config
                    )

            job.result()  # Waits for the job to complete.

            table = self.bq_client.get_table(table_id)
        except NotFound as e:
             print (e)
        except ClientError as e:
             print (e)
        
    def export_to_csv(self, query, address_to_save, view_name):
        if query != '' and query != None:
            query_job = self.bq_client.query(query)    
            df = query_job.to_dataframe()
            
            if os.path.exists(address_to_save):
               df.to_csv(f"{address_to_save}{os.sep}result_{view_name}", index=False)

    def create_or_update_view(
        self, project: str, dataset: str, view_name: str, sql_schema: str
    ) -> Table:
        """
        Function creates/updates provided sql schema to the structure.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            view_name (str):
                ID of the view.
            sql_schema (str):
                An object of internal TableSchema class.

        Returns:
            Table: An object of BigQuery Table.
        """
        self.bq_client.project = project

        try:
            bq_structure = self.get_structure(project, dataset, view_name)
            bq_structure.view_query = sql_schema
            self.bq_client.update_table(bq_structure, ["view_query"])
        except NotFound:
            bq_structure = bigquery.Table(f"{project}.{dataset}.{view_name}")
            bq_structure.view_query = sql_schema
            self.bq_client.create_table(bq_structure)

        return bq_structure
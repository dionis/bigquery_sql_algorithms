import os
from gbq import BigQuery
from pora_bigquery import PoraBigquery

BIGQUERY_DATASET = 'import_dev'
MAX_FILES_IN_ALGORITMDIR = 3
ERROR_MAX_FILES_IN_ALGORITMDIR = 'A size of files more than permit in algorithm directory.'
TABLE_NAME = 'tables'
ERROR_TABLE_NAME_NOT_FOUND = 'The name of table directory is not corret or not exist.'
LIMIT_FILE_INTABLE = 5
ERROR_LIMIT_FILE_INTABLE = 'The limit of files in table directory is more than permit.'
CORRECT_TABLE_EXTENSION = ['csv', 'json']
ERROR_CORRECT_TABLE_EXTENSION = 'The file for temporal table is not correct.'
DOC_DIRECTORY = "WP"

SQL_DROP_TABLE = "DROP TABLE IF EXISTS "

def get_squema_directory():
    return os.environ.get("dataset_schema_directory") if os.environ.get("dataset_schema_directory") != None  else BIGQUERY_DATASET


def process_source_code_in_BigQuery(bigquery_dataset:str, process_algorithm: dict):
        print(f"*** Information about PORA algorithm source code for dataset {bigquery_dataset} ***\n")
        #print(process_algorithm)

        table_name = csv_file_toload_bigquery = ''

        dataset_schema_directory =  get_squema_directory()
        dataset_schema_directory = dataset_schema_directory.split("/").pop()
        credentials = os.environ.get("credentials")
        gcp_project = os.environ.get("gcp_project")

        svc_account = credentials

        bq = PoraBigquery(svc_account=svc_account, project = gcp_project)

        # 1- Create temporal table in Biguery squeme
        if TABLE_NAME in process_algorithm:
        
            #Extract information from table directory
            list_of_tables = process_algorithm[TABLE_NAME]
            dict_file_in_table = {}
            print ('There are Table ', list_of_tables)
            
            for iTable in list_of_tables:
                itable_name, icsv_file_toload_bigquery = iTable
                ifile_name, icsv_file_extension = os.path.splitext(icsv_file_toload_bigquery)
                
                for jTable in list_of_tables:
                    jtable_name, jcsv_file_toload_bigquery = jTable
                    jfile_name, jcsv_file_extension = os.path.splitext(jcsv_file_toload_bigquery)
                    
                    print(f" itable_name {itable_name} and icsv_file {icsv_file} Vs jtable_name {jtable_name} and jcsv_file {jcsv_file}")
                    
                    if jtable_name == itable_name  and icsv_file_extension != jcsv_file_extension and itable_name not in dict_file_in_table:
                         dict_file_in_table[itable_name] = (icsv_file_toload_bigquery, jcsv_file_toload_bigquery)
                        
            print(f"Data in tables {dict_file_in_table}")
            
            for table_name, value in dict_file_in_table.items():
                csv_file_toload_bigquery,  json_file_toload_bigquery = value
                print(f"Step 0 - Drop table {table_name} if exist  `{SQL_DROP_TABLE} {dataset_schema_directory}.{table_name}`\n")

                bq.execute(
                        f"{SQL_DROP_TABLE} {dataset_schema_directory}.{table_name}"
                    )

                print(f"Step 1 - Create table {table_name} in BigQuery using csv source code {csv_file_toload_bigquery}\n")
                print(f"Step 1.1 - Create table {table_name} in BigQuery using JSON source code {json_file_toload_bigquery}\n")
                
                # import json 
                
                # with open(json_file_toload_bigquery) as f:
                #     json_data = json.load(f)
                    
                #     for key, value in json_data.items():
                #         print(f" in Dict Json value: {key} and {value}")
               
                    
                bq.bigquery_import_csv(csv_file_toload_bigquery, json_file_toload_bigquery, f"{gcp_project}.{dataset_schema_directory}.{table_name}", dataset_schema_directory)
            
            #erase in dictornary
            del process_algorithm[TABLE_NAME]
        
        #Update algoritm code in Bigquery
        for key, ialgorithm_source_address in process_algorithm.items():
            print(f"Step 2- Create or Update PORA algorithm {key} in Bigquery with file in address {ialgorithm_source_address}\n")

            #Execute algoritm for testing propouse
            print(f"Step 3- Execute PORA algorithm {key} in Bigquery with file in address {ialgorithm_source_address}\n")

        #Delete temporal table
        if table_name != '':
          print(f"Setep 4- Delete temporal table {table_name} in BigQuery\n")
          #bq.delete_table(gcp_project, dataset, table_name)
        
        # 2- Import data from CSV file to temporal table in BigQuery
        # 3- Update the temporal table in BigQuery
        # 4- Create a view based on the temporal table in BigQuery
        # 5- Delete the temporal table in BigQuery
        # 6- Deploy the view in BigQuery as a new table
        


def process_algorithm_codesource(bigquery_dataset: str, algorithm_name: str, algoritm_source_address:str):
    print(f"In file addres {algoritm_source_address}")   

    list_of_files = os.listdir(algoritm_source_address)
    size_of_files = len(list_of_files)
    
    print(f" List files in directory: {list_of_files}")

    if size_of_files == 0 or size_of_files > MAX_FILES_IN_ALGORITMDIR:
        raise Exception(f"{ERROR_MAX_FILES_IN_ALGORITMDIR}: {size_of_files}")
    else:
        process_algorithm = {}

        for ifiles in  list_of_files:
            source_path = algoritm_source_address + os.sep + ifiles  

            if os.path.isdir(source_path) and ifiles != DOC_DIRECTORY:
                directory_name =  source_path.split(os.sep)[-1]

                if directory_name != TABLE_NAME:
                    raise Exception(ERROR_TABLE_NAME_NOT_FOUND)
                else:
                    ##List dir files and get only one
                    list_source_code_intable = os.listdir(source_path)

                    if len(list_source_code_intable) > LIMIT_FILE_INTABLE:
                        raise Exception(ERROR_LIMIT_FILE_INTABLE)
                    else: #Only a file with csv extension
                        for ifile_intable in list_source_code_intable:
                            file_name_and_extension = ifile_intable.split(".")      

                            if (file_name_and_extension[1] not in  CORRECT_TABLE_EXTENSION):
                                raise Exception(f"{ERROR_CORRECT_TABLE_EXTENSION}: {file_name_and_extension[1]}")
                            else:
                                if  TABLE_NAME not in process_algorithm:
                                   process_algorithm[TABLE_NAME] = [(
                                                file_name_and_extension[0],  
                                                source_path + os.sep + ifile_intable
                                            )]
                                else:
                                    process_algorithm[TABLE_NAME].append((
                                                file_name_and_extension[0],  
                                                source_path + os.sep + ifile_intable
                                            ))
                
            elif os.path.isfile(source_path):
                file_name_and_extension = ifiles.split(".")      
                process_algorithm[file_name_and_extension[0]] = source_path
        
        
        process_source_code_in_BigQuery(bigquery_dataset, process_algorithm)



def _evaluate_directory_files(root_directory):
    algorithm_code_list = {}

    for ifiles in  os.listdir(root_directory):
        if os.path.isdir(root_directory + os.sep + ifiles):
            print(f"Running in directory {ifiles}")
            algorithm_code_list[ifiles] = root_directory + os.sep + ifiles
        #print (ifiles)

    print (f"Algorithms directory list {algorithm_code_list}")
    print("#######################################################\n")

    for key, ialgorithm_source_addres in algorithm_code_list.items():
        process_algorithm_codesource(root_directory, key, ialgorithm_source_addres)
        print("________________________________________________________________\n")

    print("Finished processing all directories.")

def _validate_env_variables():
    if not os.environ.get("gcp_project"):
        raise Exception("Missing `gcp_project` config")

    if not os.environ.get("dataset_schema_directory"):
        raise Exception("Missing `dataset_schema_directory` config")

    if not os.environ.get("credentials"):
        raise Exception("Missing `credentials` config")

def _validate_if_path_exists():
    dataset_schema_directory = get_squema_directory()
    
    print(f"Directory path to file {dataset_schema_directory}")
    return os.path.isdir(dataset_schema_directory)

def main():
    _validate_env_variables()
    print("-----------------")

    if _validate_if_path_exists():
       print("------====================================-----------")
       bigquery_datasets = get_squema_directory()
       _evaluate_directory_files(bigquery_datasets)
       print("------====================================-----------")
    else:
        raise DatasetSchemaDirectoryNonExistent
  


if __name__ == "__main__":
    main()
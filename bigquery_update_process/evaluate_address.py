import os

bigquery_datasets = 'bigquery_update_process/test_pora_bigquery/'
MAX_FILES_IN_ALGORITMDIR = 2
ERROR_MAX_FILES_IN_ALGORITMDIR = 'A size of files more than permit in algorithm directory'
TABLE_NAME = 'tables'
ERROR_TABLE_NAME_NOT_FOUND = 'The name of table directory is not corret or not exist'
LIMIT_FILE_INTABLE = 1
ERROR_LIMIT_FILE_INTABLE = 'The limit of files in table directory is more than permit'
CORRECT_TABLE_EXTENSION = 'csv'
ERROR_CORRECT_TABLE_EXTENSION = 'The file for temporal table is not correct '

def process_source_code_in_BigQuery(bigquery_dataset:str, process_algorithm: dict):
        print(f"Information about PORA algorithm source code for dataset {bigquery_dataset}")
        #print(process_algorithm)

        table_name = csv_file_toload_bigquery = ''

        # 1- Create temporal table in Biguery squeme
        if TABLE_NAME in process_algorithm:
            #erase in dictornary
            table_name,  csv_file_toload_bigquery = process_algorithm[TABLE_NAME]
            print(f"Step 0 - Create table {table_name} in BigQuery using csv source code {csv_file_toload_bigquery}")
            #bq.create_table_from_csv(gcp_project, dataset, table_name, csv_file_toload_bigquery)
            del process_algorithm[TABLE_NAME]
        
        #Update algoritm code in Bigquery
        for key, ialgorithm_source_address in process_algorithm.items():
            print(f"Step 1- Create or Update PORA algorithm {key} in Bigquery with file in address {ialgorithm_source_address}")

            #Execute algoritm for testing propouse
            print(f"Step 2- Execute PORA algorithm {key} in Bigquery with file in address {ialgorithm_source_address}")

        #Delete temporal table
        if table_name != '':
          print(f"Setep 3- Delete temporal table {table_name} in BigQuery")
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

    if size_of_files == 0 or size_of_files > MAX_FILES_IN_ALGORITMDIR:
        raise Exception(f"{ERROR_MAX_FILES_IN_ALGORITMDIR}: {size_of_files}")
    else:
        process_algorithm = {}

        for ifiles in  os.listdir(algoritm_source_address):
            source_path = algoritm_source_address + os.sep + ifiles

            if os.path.isdir(source_path):
                directory_name =  source_path.split(os.sep)[-1]

                if directory_name != TABLE_NAME:
                    raise Exception(ERROR_TABLE_NAME_NOT_FOUND)
                else:
                    ##List dir files and get only one
                    list_source_code_intable = os.listdir(source_path)

                    if len(list_source_code_intable) > LIMIT_FILE_INTABLE:
                        raise Exception(ERROR_LIMIT_FILE_INTABLE)
                    else: #Only a file with csv extension
                        file_name_and_extension = list_source_code_intable[0].split(".")      

                        if (file_name_and_extension[1] != CORRECT_TABLE_EXTENSION):
                            raise Exception(f"{ERROR_CORRECT_TABLE_EXTENSION}: {file_name_and_extension[1]}")
                        else:
                            process_algorithm[TABLE_NAME] =(file_name_and_extension[0],  source_path + os.sep + list_source_code_intable[0])
                    
                
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


def main():
    _evaluate_directory_files(bigquery_datasets)


if __name__ == "__main__":
    main()
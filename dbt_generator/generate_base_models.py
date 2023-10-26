import os
import yaml
import subprocess
from platform import system


def get_base_tables_and_source(file_path, source_index):
    file = open(file_path)
    sources = yaml.load(file, Loader=yaml.FullLoader)
    tables_configs = sources['sources'][source_index]['tables']
    table_names = [item['name'] for item in tables_configs]
    source_name = sources['sources'][source_index]['name']
    return table_names, source_name

def extract_snapshot_info(source_system):
    print(f'Retrieving query results from dbt_snapshot_seed where source_system = "{source_system}"')
    bash_command = f'''dbt run-operation macro_snapshot_list --args "source_system: {source_system}"'''
    
    if system() == 'Windows':
        output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
    else:
        output = subprocess.check_output(bash_command, shell=True).decode("utf-8")
    
    sql_index = output.upper().find("{'SYSTEM_NAME")
    query_results = output[sql_index:]
    return query_results

def get_snapshot_sql(snapshot_name, dbt_source, table_name, unique_key, snapshot_strategy="timestamp", updated_at="None", check_cols="None", invalidate_hard_deletes=0, composite_key=0):
    print(f'Getting snapshot code for {dbt_source}.{table_name} using snapshot strategy {snapshot_strategy}.')
    bash_command = f'''
        dbt run-operation macro_create_snapshot --args \'{{"snapshot_name": "{snapshot_name}", "dbt_source": "{dbt_source}", "table_name": "{table_name}", "unique_key": "{unique_key}", "snapshot_strategy": "{snapshot_strategy}", "updated_at": "{updated_at}", "check_cols": "{check_cols}", "invalidate_hard_deletes": {invalidate_hard_deletes}, "composite_key": {composite_key} }}\'
    '''
    
    if system() == 'Windows':
        output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
    else:
        output = subprocess.check_output(bash_command, shell=True).decode("utf-8")
    
    sql_index = output.lower().find("{% snapshot")
    query_results = output[sql_index:]
    return query_results

def get_soft_delete_snapshot_sql(snapshot_name, invalidate_fivetran_soft_deletes, invalidate_soft_deletes, soft_delete_indicator_col, soft_delete_date_col):
    print(f'Getting soft-delete snapshot code for snapshot {snapshot_name}.')
    bash_command = f'''
        dbt run-operation macro_create_soft_delete_snapshot --args \'{{"snapshot_name": "{snapshot_name}", "invalidate_fivetran_soft_deletes": {invalidate_fivetran_soft_deletes}, "invalidate_soft_deletes": {invalidate_soft_deletes}, "soft_delete_indicator_col": "{soft_delete_indicator_col}", "soft_delete_date_col": "{soft_delete_date_col}" }}\'
    '''

    if system() == 'Windows':
        output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
    else:
        output = subprocess.check_output(bash_command, shell=True).decode("utf-8")

    sql_index = output.lower().find("select")
    query_results = output[sql_index:]
    return query_results

def generate_base_model(table_name, source_name, case_sensitive, leading_commas, materialized, use_snapshot):
    print(f'Generating base model for table {table_name}')
    bash_command = f'''
        dbt run-operation macro_create_base_model --args \'{{"source_name": "{source_name}", "table_name": "{table_name}", "case_sensitive_cols": {case_sensitive}, "leading_commas": {leading_commas}, "materialized": "{materialized}", "use_snapshot": {use_snapshot} }}\'
    '''
    if system() == 'Windows':
        output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
    else:
        output = subprocess.check_output(bash_command, shell=True).decode("utf-8")

    if materialized == '':
        sql_index = output.lower().find('with source as')
    else:
        sql_index = output.lower().find('{{ config(materialized=')
    sql_query = output[sql_index:]
    return sql_query

def generate_source_yaml(database_name, schema_name, table_names, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, include_database, include_schema):
    fq_path = database_name + '.' + schema_name
    print(f'Generating source yaml for schema {fq_path}')
    # start the bash command
    bash_command = f'''
        dbt run-operation generate_source  --args \'{{"database_name": "{database_name}", "schema_name": "{schema_name}", "generate_columns": {generate_columns}, "include_descriptions": {include_descriptions}, "name": "{name}", "include_database": {include_database}, "include_schema": {include_schema}'''

    if table_names: # add the table names arg
        bash_command = f'''{bash_command}, "table_names": "{table_names}" '''
    if exclude: # add the exclusions
        bash_command = f'''{bash_command}, "exclude": "{exclude}" '''
    if table_pattern: # add the table_pattern
        bash_command = f'''{bash_command}, "table_pattern": "{table_pattern}" '''

    # close the bash command
    bash_command = bash_command + " }\'"

    if system() == 'Windows':
        output = subprocess.check_output(["powershell.exe",bash_command]).decode("utf-8")
    else:
        output = subprocess.check_output(bash_command, shell=True).decode("utf-8")
    
    sql_index = output.lower().find('version') # Trim any excess output from the beginning "version: 2" should be the first line
    sql_query = output[sql_index:]
    return sql_query
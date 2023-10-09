import os
import click
from .generate_base_models import *
from .process_base_models import *
import ast
import pandas as pd


def get_file_name(file_path):
    return os.path.basename(file_path)

@click.group(help='Generate and process base dbt models')
def dbt_generator():
    pass

@dbt_generator.command(help='Generate base models based on a .yml source')
@click.option('-s', '--source-yml', type=click.Path(), help='Source .yml file to be used')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write generated models')
@click.option('-m', '--model', type=str, default='', help='Select one model to generate')
@click.option('-c', '--custom_prefix', type=str, default='', help='Enter a Custom String Prefix for Model Filename')
@click.option('--model-prefix', type=bool, default=False, help='Prefix model name with source_name + _')
@click.option('--source-index', type=int, default=0, help='Index of the source to generate base models for')
@click.option('--model-prefix', type=bool, default=False, help='Prefix model name with source_name + _')
@click.option('--case-sensitive', type=bool, help='(default=False) treat column names as case-sensitive - otherwise force all to lower', default=False)
@click.option('--leading-commas', type=bool, help='(default=False)  Whether you want your commas to be leading (vs trailing).', default=False)
@click.option('--materialized', type=str, default='', help='Set materialization style (e.g. table, view, incremental) inside of the model config block. If not set, materialization style will be controlled by dbt_project.yml')
def generate(source_yml, output_path, source_index, model, custom_prefix, model_prefix, case_sensitive, leading_commas, materialized):
    tables, source_name = get_base_tables_and_source(source_yml, source_index)
    if model:
        tables = [model]
    for table in tables:
        file_name = table + '.sql'
        if model_prefix:
            file_name = source_name + '_' + file_name
        if custom_prefix:
            file_name = custom_prefix + '_' + file_name
        
        query = generate_base_model(table, source_name, case_sensitive, leading_commas, materialized)
        file = open(os.path.join(output_path, file_name), 'w', newline='')
        file.write(query)

@dbt_generator.command(help='Transform base models in a directory using a transforms.yml file')
@click.option('-m', '--model-path', type=click.Path(), help='The path to models')
@click.option('-t', '--transforms-path', type=click.Path(), help='Path to a .yml file containing transformations')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write transformed models to')
@click.option('--drop-metadata', type=bool, help='Optionally drop source columns prefixed with "_" if that designates metadata columns not needed in target', default=True)
@click.option('--case-sensitive', type=bool, help='(default=False) treat column names as case-sensitive - otherwise force all to lower', default=False)
def transform(model_path, transforms_path, output_path, drop_metadata, case_sensitive):
    sql_files = get_sql_files(model_path)
    for sql_file in sql_files:
        processor = ProcessBaseModelsWithTransforms(os.path.join(
            model_path, sql_file), transforms_path, drop_metadata, case_sensitive)
        processor.process_base_models(os.path.join(output_path, sql_file))


@dbt_generator.command(help='Transform one base model using a transforms.yml file')
@click.option('-m', '--model-path', type=click.Path(), help='The path to one single model')
@click.option('-t', '--transforms-path', type=click.Path(), help='Path to a .yml file containing transformations')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write transformed models to')
@click.option('--drop-metadata', type=bool, help='Toptionally drop source columns prefixed with "_" if that designates metadata columns not needed in target', default=True)
@click.option('--case-sensitive', type=bool, help='(default=False) treat column names as case-sensitive - otherwise force all to lower', default=False)
def transforms(model_path, transforms_path, output_path, drop_metadata, case_sensitive):
    file_name = get_file_name(model_path)
    processor = ProcessBaseModelsWithTransforms(
        model_path, transforms_path, drop_metadata, case_sensitive)
    processor.process_base_models(os.path.join(output_path, file_name))


@dbt_generator.command(help='Transform base models in a directory for BigQuery source')
@click.option('-m', '--model-path', type=click.Path(), help='The path to models')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write transformed models to')
@click.option('--drop-metadata', type=bool, help='Toptionally drop source columns prefixed with "_" if that designates metadata columns not needed in target', default=True)
@click.option('--case-sensitive', type=bool, help='(default=False) treat column names as case-sensitive - otherwise force all to lower', default=False)
@click.option('--split-columns', type=bool, help='Split column names. E.g. currencycode => currency_code', default=False)
@click.option('--id-as-int', type=bool, help='Convert id to int', default=False)
@click.option('--convert-timestamp', type=bool, help='Convert timestamp to datetime', default=False)
def bq_transform(model_path, output_path, drop_metadata, case_sensitive, split_columns, id_as_int, convert_timestamp):
    sql_files = get_sql_files(model_path)
    for sql_file in sql_files:
        processor = ProcessBaseModelsBQ(os.path.join(
            model_path, sql_file), drop_metadata, case_sensitive, split_columns, id_as_int, convert_timestamp)
        processor.process_base_models(os.path.join(output_path, sql_file))


@dbt_generator.command(help='Transform base models in a directory for Snowflake source')
@click.option('-m', '--model-path', type=click.Path(), help='The path to models')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write transformed models to')
@click.option('--drop-metadata', type=bool, help='Toptionally drop source columns prefixed with "_" if that designates metadata columns not needed in target', default=True)
@click.option('--case-sensitive', type=bool, help='(default=False) treat column names as case-sensitive - otherwise force all to lower', default=False)
@click.option('--split-columns', type=bool, help='Split column names. E.g. currencycode => currency_code', default=False)
@click.option('--id-as-int', type=bool, help='Convert id to int', default=False)
@click.option('--convert-timestamp', type=bool, help='Convert timestamp to datetime', default=False)
def sf_transform(model_path, output_path, drop_metadata, case_sensitive, split_columns, id_as_int, convert_timestamp):
    sql_files = get_sql_files(model_path)
    for sql_file in sql_files:
        processor = ProcessBaseModelsSF(os.path.join(
            model_path, sql_file), drop_metadata, case_sensitive, split_columns, id_as_int, convert_timestamp)
        processor.process_base_models(os.path.join(output_path, sql_file))

@dbt_generator.command(help='Generate source .yml.')
@click.option('-o', '--output-path', type=click.Path(), help='Path to write generated .yml')
@click.option('-c', '--custom_prefix', type=str, default='', help='Enter a Custom String Prefix for Model Filename')
@click.option('--model-prefix', type=bool, default=False, help='Prefix .yml name with source_name + _')
@click.option('--schema_name', type=str, default='', help='(required): The schema name that contains your source data')
@click.option('--database_name', type=str, default='', help='(required): The database that your source data is in.')
@click.option('--table_names', type=str, default='', help='(optional, default=none): A list of tables that you want to generate the source definitions for (i.e. ["table_1", "table_2"] )')
@click.option('--generate_columns', type=bool, default=False, help='(optional, default=False): Whether you want to add the column names to your source definition.')
@click.option('--include_descriptions', type=bool, default=False, help='(optional, default=False): Whether you want to add description placeholders to your source definition.')
@click.option('--include_data_types', type=bool, default=True, help='(optional, default=True): Whether you want to add data types to your source columns definitions.')
@click.option('--table_pattern', type=str, default='', help='(optional, default="%"): A table prefix / postfix that you want to subselect from all available tables within a given schema.')
@click.option('--exclude', type=str, default='', help='(optional, default=''): A string you want to exclude from the selection criteria')
@click.option('--name', type=str, default='', help='(optional, default=schema_name): The name of your source')
@click.option('--include_database', type=bool, default=False, help='(optional, default=False): Whether you want to add the database to your source definition')
@click.option('--include_schema', type=bool, default=False, help='(optional, default=False): Whether you want to add the schema to your source definition')
def source_yaml(output_path, custom_prefix, model_prefix, database_name, schema_name, table_names, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, include_database, include_schema):
    if name == '': # default behavior for the function
        name = schema_name
    
    file_name = schema_name + '.yml'
    if model_prefix:
        file_name = database_name + '_' + file_name
    if custom_prefix:
        file_name = custom_prefix + '_' + file_name
        
    query = generate_source_yaml(database_name, schema_name, table_names, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, include_database, include_schema)
    file = open(os.path.join(output_path, file_name), 'w', newline='')
    file.write(query)

@dbt_generator.command(help='Generate snapshots based on table. See DBT_SNAPSHOT_SEED.CSV in seeds directory.')
@click.option('--system_name', type=str, default='', help='(required): The system_name that contains your source data. See dbt_snapshot_seed.csv seed file.')
@click.option('-o', '--output-path', type=click.Path(), help='(required):Path to write generated snapshot files.')
@click.option('--target_schema', type=str, default='', help='(required): The name of the schema where you want your snapshots to be created by DBT.')
def generate_snapshots(system_name, output_path, target_schema):
    query_results = extract_snapshot_info(system_name)
    results_dict = ast.literal_eval(query_results) # Need to convert this back to a dict due to how we are retrieving it.
    results_df = pd.DataFrame.from_dict(results_dict)
    # iterate over dataframe and generate the actual sql files in the output folder
    for index, row in results_df.iterrows():
        contents = get_snapshot_sql(dbt_source=row['DBT_SOURCE'].lower(), target_schema=target_schema.lower(), table_name=row['TABLE_NAME'].lower(), unique_key=row['UNIQUE_KEY'].lower(), snapshot_strategy=row['SNAPSHOT_STRATEGY'].lower(), updated_at=row['UPDATED_AT'], check_cols=row['CHECK_COLS'], invalidate_hard_deletes=row['INVALIDATE_HARD_DELETES'], composite_key=row['COMPOSITE_KEY'])
        file_name = "snp_" + row['DBT_SOURCE'].lower() + "_" + row['TABLE_NAME'].lower() + ".sql"

        file = open(os.path.join(output_path, file_name), 'w', newline='')
        file.write(contents)

if __name__ == '__main__':
    dbt_generator()

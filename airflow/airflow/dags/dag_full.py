#dag_full
# -*- coding: utf-8 -*-

"""
Title: Project Dag 
Author: Nagel
Description: 
Dag to perform all needed jobs related to the towers_full
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator

args = {
    'owner': 'airflow'
}
#create dag
dag = DAG('cell_towers', default_args=args, description='Project',
          schedule_interval='@once',
          start_date=datetime(2022, 11, 21), catchup=False, max_active_runs=1)
#create dir
create_local_import_dir = CreateDirectoryOperator(
     task_id='create_import_dir',
     path='/home/airflow',
     directory='opencellid',
     dag=dag,
)
#create sub-dir
create_local_import_dir_2 = CreateDirectoryOperator(
    task_id='create_import_dir_2',
    path='/home/airflow/opencellid',
    directory='raw',
    dag=dag,
)
#clear dir
clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/opencellid/raw',
    pattern='*',
    dag=dag,
)
#######
#load data from ocid
download_cell_towers = HttpDownloadOperator(
    task_id='download_cell_towers',
    download_uri='https://opencellid.org/ocid/downloads?token=pk.2b0ffe67bffaf2bc09c4fe8fd1b17d45&type=full&file=cell_towers.csv.gz',
    save_to='/home/airflow/opencellid/raw/cell_towers.csv.gz',
    dag=dag,
)
#full unzip
unzip_cell_towers = UnzipFileOperator(
    task_id='unzip_cell_towers',
    zip_file='/home/airflow/opencellid/raw/cell_towers.csv.gz',
    extract_to='/home/airflow/opencellid/raw/cell_towers.csv',
    dag=dag,
)
#hdfs dir for partitions
create_hdfs_cell_towers_partition_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_cell_towers_partition_dir',
    directory='/user/hadoop/opencellid/cell_towers',
    hdfs_conn_id='hdfs',
    dag=dag,
)

#move file to hdfs
hdfs_put_tower_cells = HdfsPutFileOperator(
    task_id='upload_tower_cells_to_hdfs',
    local_file='/home/airflow/opencellid/raw/cell_towers.csv',
    remote_file='/user/hadoop/opencellid/cell_towers/cell_towers.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)
pyspark = SparkSubmitOperator(
    task_id='pyspark',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_full.py',
    total_executer_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='ocid_pyspark',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/opencellid/cell_towers', '--hdfs_target_dir', '/user/hadoop/opencellid/final', '--hdfs_target_format', 'csv'],
    dag = dag
)
dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)
###run DAG
dummy_op
create_local_import_dir >> create_local_import_dir_2 >> clear_local_import_dir
clear_local_import_dir  >> download_cell_towers >> unzip_cell_towers >>create_hdfs_cell_towers_partition_dir >> hdfs_put_tower_cells >> pyspark
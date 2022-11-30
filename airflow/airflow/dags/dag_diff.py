#dag_diff
# -*- coding: utf-8 -*-

"""
Title: Project Dag 
Author: Nagel
Description: 
Dag to perform all needed jobs for diffs
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'airflow'
}

#create dag
dag = DAG('cell_towers_diff', default_args=args, description='Project',
          schedule_interval='56 18 * * *',
          start_date=datetime(2022, 11, 21), catchup=False, max_active_runs=1)

#create dir
create_local_import_diff_dir = CreateDirectoryOperator(
     task_id='create_import_diff_dir',
     path='/home/airflow',
     directory='ocid_diff',
     dag=dag,
)
#create sub-dir
create_local_import_diff_dir_2 = CreateDirectoryOperator(
    task_id='create_import_diff_dir_2',
    path='/home/airflow/ocid_diff',
    directory='raw',
    dag=dag,
)
#clear dir
clear_local_import_diff_dir = ClearDirectoryOperator(
    task_id='clear_import_diff_dir',
    directory='/home/airflow/ocid_diff/raw',
    pattern='*',
    dag=dag,
)

#######
#load data from ocid
download_cell_towers_diff = HttpDownloadOperator(
    task_id='download_cell_towers_diff',
    download_uri='https://onedrive.live.com/download?cid=6CD9C3F4D2E50BCB&resid=6CD9C3F4D2E50BCB%2159291&authkey=ANu-4_qT3NxqPqo', #muss durch die offizielle URL getauscht werden!
    save_to='/home/airflow/ocid_diff/raw/cell_towers_diff.csv.gz',
    dag=dag,
)
#full unzip
unzip_cell_towers_diff = UnzipFileOperator(
    task_id='unzip_cell_towers_diff',
    zip_file='/home/airflow/ocid_diff/raw/cell_towers_diff.csv.gz',
    extract_to='/home/airflow/ocid_diff/raw/cell_towers_diff.csv',
    dag=dag,
)

#hdfs dir for partitions
create_hdfs_cell_towers_diff_partition_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_cell_towers_diff_partition_dir',
    directory='/user/hadoop/ocid_diff/cell_towers_diff',
    hdfs_conn_id='hdfs',
    dag=dag,
)

#move file to hdfs
hdfs_put_tower_cells_diff = HdfsPutFileOperator(
    task_id='upload_tower_cells_diff_to_hdfs',
    local_file='/home/airflow/ocid_diff/raw/cell_towers_diff.csv',
    remote_file='/user/hadoop/ocid_diff/cell_towers_diff/cell_towers_diff.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

pyspark_diff = SparkSubmitOperator(
    task_id='pyspark',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_diff.py',
    total_executer_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='ocid_pyspark_diff',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/ocid_diff/cell_towers_diff', '--hdfs_target_dir', '/user/hadoop/ocid_diff/final', '--hdfs_target_format', 'csv'],
    dag = dag
)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

###run DAG
dummy_op
create_local_import_diff_dir >> create_local_import_diff_dir_2 >> clear_local_import_diff_dir
clear_local_import_diff_dir  >> download_cell_towers_diff >> unzip_cell_towers_diff >>create_hdfs_cell_towers_diff_partition_dir >> hdfs_put_tower_cells_diff >> pyspark_diff
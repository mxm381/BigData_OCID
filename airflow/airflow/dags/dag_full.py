# -*- coding: utf-8 -*-

"""
Title: Project Dag 
Author: Nagel
Description: 
Dag to perform all needed jobs for project
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

#create full table
hiveSQL_create_table_full_db_cell_towers='''
CREATE EXTERNAL TABLE IF NOT EXISTS full_db_cell_towers(
    radio STRING,
    mcc INT,
    net INT,
    area INT,
    cell INT,
    unit INT,
    lon DECIMAL(8,6),
    lat DECIMAL(8,6),
    ranged INT,
    samples INT,
    changeable INT,
    created INT,
    updated INT,
    averageSignal INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/opencellid/cell_towers'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

####create partition tables####
#create partition UMTS
hiveSQL_create_table_partitioned_UMTS='''
CREATE TABLE IF NOT EXISTS partitioned_db_cell_towers(
    radio STRING,
    mcc INT,
    net INT,
    area INT,
    cell INT,
    unit INT,
    lon DECIMAL(8,6),
    lat DECIMAL(8,6),
    ranged INT,
    samples INT,
    changeable INT,
    created INT,
    updated INT,
    averageSignal INT
) PARTITIONED BY (partition_radio STRING) STORED AS PARQUET LOCATION '/user/hadoop/opencellid/raw';
'''

#create partition CDMA
hiveSQL_create_table_partitioned_CDMA='''
CREATE TABLE IF NOT EXISTS partitioned_db_cell_towers(
    radio STRING,
    mcc INT,
    net INT,
    area INT,
    cell INT,
    unit INT,
    lon DECIMAL(8,6),
    lat DECIMAL(8,6),
    ranged INT,
    samples INT,
    changeable INT,
    created INT,
    updated INT,
    averageSignal INT
) PARTITIONED BY (partition_radio STRING) STORED AS PARQUET LOCATION '/user/hadoop/opencellid/raw';
'''

#create partition GMS
hiveSQL_create_table_partitioned_GMS='''
CREATE TABLE IF NOT EXISTS partitioned_db_cell_towers(
    radio STRING,
    mcc INT,
    net INT,
    area INT,
    cell INT,
    unit INT,
    lon DECIMAL(8,6),
    lat DECIMAL(8,6),
    ranged INT,
    samples INT,
    changeable INT,
    created INT,
    updated INT,
    averageSignal INT
) PARTITIONED BY (partition_radio STRING) STORED AS PARQUET LOCATION '/user/hadoop/opencellid/raw';
'''

#create partition LTE
hiveSQL_create_table_partitioned_LTE='''
CREATE TABLE IF NOT EXISTS partitioned_db_cell_towers(
    radio STRING,
    mcc INT,
    net INT,
    area INT,
    cell INT,
    unit INT,
    lon DECIMAL(8,6),
    lat DECIMAL(8,6),
    ranged INT,
    samples INT,
    changeable INT,
    created INT,
    updated INT,
    averageSignal INT
) PARTITIONED BY (partition_radio STRING) STORED AS PARQUET LOCATION '/user/hadoop/opencellid/raw';
'''

####fill partition tables####
#fill partition UMTS
hiveSQL_add_partition_radio_UMTS='''
INSERT OVERWRITE TABLE partitioned_db_cell_towers PARTITION(partition_radio='UMTS')
SELECT * FROM full_db_cell_towers r WHERE r.radio == 'UMTS';
'''

#fill partition GSM
hiveSQL_add_partition_radio_GSM='''
INSERT OVERWRITE TABLE partitioned_db_cell_towers PARTITION(partition_radio='GSM')
SELECT * FROM full_db_cell_towers r WHERE r.radio == 'GSM';
'''

#fill partition CMDA
hiveSQL_add_partition_radio_CDMA='''
INSERT OVERWRITE TABLE partitioned_db_cell_towers PARTITION(partition_radio='CDMA')
SELECT * FROM full_db_cell_towers r WHERE r.radio == 'CDMA';
'''

#fill partition LTE
hiveSQL_add_partition_radio_LTE='''
INSERT OVERWRITE TABLE partitioned_db_cell_towers PARTITION(partition_radio='LTE')
SELECT * FROM full_db_cell_towers r WHERE r.radio == 'LTE';
'''

#create dag
dag = DAG('cell_towers', default_args=args, description='Project',
          schedule_interval='56 18 * * *',
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
#full datei von opencellid laden
download_cell_towers = HttpDownloadOperator(
    task_id='download_cell_towers',
    download_uri='https://onedrive.live.com/download?cid=6CD9C3F4D2E50BCB&resid=6CD9C3F4D2E50BCB%2159290&authkey=AMinp5rC36d7X4k', #muss durch die offizielle URL getauscht werden!
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

#dir for partitions
create_hdfs_cell_towers_partition_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_cell_towers_partition_dir',
    directory='/user/hadoop/opencellid/cell_towers',
    hdfs_conn_id='hdfs',
    dag=dag,
)

#move file
hdfs_put_tower_cells = HdfsPutFileOperator(
    task_id='upload_tower_cells_to_hdfs',
    local_file='/home/airflow/opencellid/raw/cell_towers.csv',
    remote_file='/user/hadoop/opencellid/cell_towers/cell_towers.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

#create hive table full
create_HiveTable_full_db_cell_towers = HiveOperator(
    task_id='create_cell_towers_table',
    hql=hiveSQL_create_table_full_db_cell_towers,
    hive_cli_conn_id='beeline',
    dag=dag)

#create hive table partitioned UMTS
create_table_partitioned_UMTS = HiveOperator(
    task_id='create_UMTS_partitioned_table',
    hql=hiveSQL_create_table_partitioned_UMTS,
    hive_cli_conn_id='beeline',
    dag=dag)

#create hive table partitioned GMS
create_table_partitioned_GMS = HiveOperator(
    task_id='create_GMS_partitioned_table',
    hql=hiveSQL_create_table_partitioned_GMS,
    hive_cli_conn_id='beeline',
    dag=dag)

#create hive table partitioned CDMA
create_table_partitioned_CDMA = HiveOperator(
    task_id='create_CDMA_partitioned_table',
    hql=hiveSQL_create_table_partitioned_CDMA,
    hive_cli_conn_id='beeline',
    dag=dag)

#create hive table partitioned LTE
create_table_partitioned_LTE = HiveOperator(
    task_id='create_LTE_partitioned_table',
    hql=hiveSQL_create_table_partitioned_LTE,
    hive_cli_conn_id='beeline',
    dag=dag)

#create hive table UMTS
addPartition_HiveTable_UMTS = HiveOperator(
    task_id='add_partition_UMTS_table',
    hql=hiveSQL_add_partition_radio_UMTS,
    hive_cli_conn_id='beeline',
    dag=dag)

#create hive table GSM
addPartition_HiveTable_GSM = HiveOperator(
    task_id='add_partition_GSM_table',
    hql=hiveSQL_add_partition_radio_GSM,
    hive_cli_conn_id='beeline',
    dag=dag)

#create hive table CDMA
addPartition_HiveTable_CDMA = HiveOperator(
    task_id='add_partition_CDMA_table',
    hql=hiveSQL_add_partition_radio_CDMA,
    hive_cli_conn_id='beeline',
    dag=dag)

#create hive table LTE
addPartition_HiveTable_LTE = HiveOperator(
    task_id='add_partition_LTE_table',
    hql=hiveSQL_add_partition_radio_LTE,
    hive_cli_conn_id='beeline',
    dag=dag)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

###run DAG
dummy_op
create_local_import_dir >> create_local_import_dir_2 >> clear_local_import_dir
clear_local_import_dir  >> download_cell_towers >> unzip_cell_towers
create_hdfs_cell_towers_partition_dir >> hdfs_put_tower_cells
hdfs_put_tower_cells >> create_HiveTable_full_db_cell_towers >> hiveSQL_create_table_partitioned_UMTS >> hiveSQL_create_table_partitioned_CDMA
hiveSQL_create_table_partitioned_CDMA >> hiveSQL_create_table_partitioned_GMS >> hiveSQL_create_table_partitioned_LTE
hiveSQL_create_table_partitioned_LTE >> create_table_partitioned_CDMA >> create_table_partitioned_GMS >> create_table_partitioned_LTE >> create_table_partitioned_UMTS
create_table_partitioned_UMTS >> addPartition_HiveTable_UMTS >> addPartition_HiveTable_GSM >> addPartition_HiveTable_CDMA >> addPartition_HiveTable_LTE
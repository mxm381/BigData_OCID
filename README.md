# BigData_OCID
submission for BigData subject

## how to use:

### 1) Setup

1.1) set up a VM (https://github.com/marcelmittelstaedt/BigData/blob/master/slides/winter_semester_2022-2023/1_Distributed-Systems_Data-Models_and_Access.pdf pages 77-80)

1.2) ssh into created VM

1.3) clone this Git (git clone https://github.com/mxm381/BigData_OCID)

1.4) sudo apt install docker-compose

1.5) cd BigData_OCID

1.6) docker-compose up -d

1.7) docker exec -it hadoop bash

1.8) sudo su hadoop

1.9) cd

1.10) stop-all.sh (in case hadoop still runs any nodenames, they will be stopped with this command, without doing this, I had difficulties with hadoop)

1.11) start-all.sh

### 2) Run the dags

2.1) Visit [IP-Adress of VM]:8080

2.2) Activate DAG dag_full

2.3) Wait until dag_full is finished

2.3) Activate DAG dag_diff

### 3) Access the data: 

3.1 Visit the website at: [IP-Adress of VM]:3000


### Runtime

The dag_diff will be triggered every day at 04:00h, as the OCID data is updated every day at 02:00h.
The data can be accessed via the website and the user always has the newest data.

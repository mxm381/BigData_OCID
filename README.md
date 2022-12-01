# BigData_OCID
submission for BigData subject 
Stuttgart, 01.12.2022, Maximilian Nagel

## how to use:

### 1) Setup

1.1) set up a VM (https://github.com/marcelmittelstaedt/BigData/blob/master/slides/winter_semester_2022-2023/1_Distributed-Systems_Data-Models_and_Access.pdf pages 77-80)

1.2) sudo apt install docker-compose

1.3) clone this Git (git clone https://github.com/mxm381/BigData_OCID)

1.4) cd BigData_OCID

1.5) docker-compose up -d

1.6) docker exec -it hadoop bash

1.7) sudo su hadoop

1.8) cd

1.9) stop-all.sh (in case hadoop still runs any nodenames, they will be stopped with this command, without doing this, I had difficulties with hadoop)

1.10) start-all.sh

### 2) Run the dags

2.1) Visit [IP-Adress of VM]:8080

2.2) Activate DAG cell_towers

2.3) Wait until cell_towers is finished

2.3) Activate DAG cell_towers_diff

### 3) Access the data: 

3.1 Enable Port 3000 in the Firewall-Settings of your VM (e.g. in GCloud console)

3.2 Visit the website at: [IP-Adress of VM]:3000


### Runtime

The cell_towers_diff will be triggered every day at 04:00h, as the OCID data is updated every day at 02:00h.
The data can be accessed via the website and the user always has the newest data.

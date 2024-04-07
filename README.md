# assignment_data_platform_team_cake

## step 1: create sftp server

docker run -p 22:22 -d emberstack/sftp --name source_sftp

docker run -p 23:22 -d emberstack/sftp --name target_sftp

connecting sftp server by FileZilla then you can try to uploads txt files or create a folder inside the sftp folder.

## step 2: create sftp_pipeline image
### if you use docker desktop, you must change the host name in config file (./sftp_pipeline/config.json) from localhost to host.docker.internal

cd sftp_pipeline

docker build -t sftp_pipeline .  

<!-- docker run sftp_pipeline -->

## step 3: set up airflow
cd ..

cd airflow

docker-compose up airflow-init

docker-compose up -d

### login airflow with this username/password
username: airflow

password: airflow

and trigger the DAG
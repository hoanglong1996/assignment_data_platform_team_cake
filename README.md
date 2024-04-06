# assignment_data_platform_team_cake

docker run -p 22:22 -d emberstack/sftp --name source_sftp

docker run -p 23:22 -d emberstack/sftp --name target_sftp

cd sftp_pipeline

docker build -t sftp_pipeline .    

docker run sftp_pipeline
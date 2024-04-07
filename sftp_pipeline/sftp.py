import paramiko
import os
import json
import glob
from tenacity import retry, wait_random_exponential, stop_after_attempt

ROOT_DIR = os.path.abspath(os.curdir)

class SFTP_data_pipeline(object):
    def __init__(self):
        print("Initializing SFTP_data_pipeline")
        self.config = None

    def setup_config(self):
        config = open( os.path.join(ROOT_DIR, "config.json") , 'r').read()
        config = json.loads(config)
        self.config = config

    @retry(wait=wait_random_exponential(multiplier=3, max=60), stop=stop_after_attempt(5))
    def read_file_from_source_sftp(self):
        try:
            source_sftp = self.config['source_sftp']
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(source_sftp['host'], source_sftp['port'], source_sftp['username'], source_sftp['password'])
            sftp = ssh.open_sftp()
            files = sftp.listdir('/sftp')
            for i, file in enumerate(files):
                sftp.get(f'/sftp/{file}', f'./temporary/{file}')
                print(f'Downloaded {file}')
            sftp.close()
            ssh.close()
        except Exception as e:
            print(e)
            raise RuntimeError(e)

    @retry(wait=wait_random_exponential(multiplier=3, max=60), stop=stop_after_attempt(5))
    def push_files_to_target_sftp(self, files):
        try:
            target_sftp = self.config['target_sftp']
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(target_sftp['host'], target_sftp['port'], target_sftp['username'], target_sftp['password'])
            sftp = ssh.open_sftp()
            for file in files:
                print(file)
                sftp.put(file, "/sftp/" + file.split('/')[-1])
            sftp.close()
            ssh.close()
        except Exception as e:
            print(e)
            raise RuntimeError(e)

    def read_files_in_temporary_folder(self):
        files = glob.glob('temporary/*')
        return files
    
    def main(self):
        self.setup_config()
        self.read_file_from_source_sftp()
        files = self.read_files_in_temporary_folder()
        print(files)
        self.push_files_to_target_sftp(files)

def main():
    pipeline = SFTP_data_pipeline()
    pipeline.main()


if __name__ == '__main__':
    main()



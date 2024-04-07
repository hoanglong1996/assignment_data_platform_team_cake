import paramiko
import os
import json
import glob
from tenacity import retry, wait_random_exponential, stop_after_attempt
import stat
import os.path

ROOT_DIR = os.path.abspath(os.curdir)

def _sftp_helper(sftp, files):
    stats = sftp.listdir_attr('.')
    files[sftp.getcwd()] = [attr.filename for attr in stats if stat.S_ISREG(attr.st_mode)]

    for attr in stats:
        if stat.S_ISDIR(attr.st_mode):  # If the file is a directory, recurse it
            sftp.chdir(attr.filename)
            _sftp_helper(sftp, files)
            sftp.chdir('..')

def filelist_recursive(sftp):
    files = {}
    _sftp_helper(sftp, files)
    return files

def mkdir_p(sftp, remote_directory):
    """Change to this directory, recursively making new folders if needed.
    Returns True if any folders were created."""
    if remote_directory == '/':
        # absolute path so change directory to root
        sftp.chdir('/')
        return
    if remote_directory == '':
        # top-level relative directory must exist
        return
    try:
        sftp.chdir(remote_directory) # sub-directory exists
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        mkdir_p(sftp, dirname) # make parent directories
        sftp.mkdir(basename) # sub-directory missing, so created it
        sftp.chdir(basename)
        return True

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
            # files = sftp.listdir()
            files = filelist_recursive(sftp)
            print('all files:' ,files)
            for directory in files.keys():
                if len(files[directory]) > 0:
                    for file in files[directory]:
                        path = (directory + '/' if directory else '/sftp/')
                        if not os.path.exists(f'.{path}'):
                            os.makedirs(f'.{path}')
                            print(f".{path} directory is created!")
                        filename = path + file
                        print(f'Downloading file from {filename}', 'to', f'.{filename}')
                        sftp.get(f'{filename}', f'.{filename}')
                        print(f'Downloaded {filename}')
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
                target_file = file.replace('./','/')
                target_path = os.path.dirname(target_file)
                print('Uploading from', file, 'to', target_file)
                mkdir_p(sftp, target_path)
                sftp.put(file, target_file)
            sftp.close()
            ssh.close()
        except Exception as e:
            print(e)
            raise RuntimeError(e)

    def read_files_in_sftp_folder(self):
        files = glob.glob('./sftp/**/*.*', recursive=True)
        return files
    
    def main(self):
        self.setup_config()
        self.read_file_from_source_sftp()
        files = self.read_files_in_sftp_folder()
        self.push_files_to_target_sftp(files)

def main():
    pipeline = SFTP_data_pipeline()
    pipeline.main()


if __name__ == '__main__':
    main()



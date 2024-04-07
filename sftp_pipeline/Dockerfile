FROM python:3.10

RUN pip install \
    'six==1.16.0' \
    'paramiko==2.8.0'\
    'tenacity==8.2.3'

ADD . /

CMD python -u sftp.py

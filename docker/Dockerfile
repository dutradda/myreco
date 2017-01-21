FROM python:3.5

MAINTAINER Diogo Dutra <dutradda@gmail.com>
RUN apt-get update
RUN apt-get install -y build-essential libopenblas-dev liblapack-dev
RUN git clone http://github.com/dutradda/myreco /tmp/myreco
RUN pip install -r /tmp/myreco/requirements-dev.txt -r /tmp/myreco/requirements.txt --src /usr/local/src
RUN rm -rf /tmp/myreco
ADD run_tests.sh /usr/bin/run-tests
ENTRYPOINT /bin/bash
FROM c06861d2891e

MAINTAINER Diogo Dutra <dutradda@gmail.com>
RUN apk update && apk add bash alpine-sdk lapack openblas-dev
RUN ln -s /usr/include/locale.h /usr/include/xlocale.h
RUN git clone http://github.com/dutradda/myreco /tmp/myreco
RUN pip install -r /tmp/myreco/requirements-dev.txt -r /tmp/myreco/requirements.txt --src /usr/local/src
RUN rm -rf /tmp/myreco
ADD run_tests.sh /usr/bin/run-tests
ENTRYPOINT /bin/bash
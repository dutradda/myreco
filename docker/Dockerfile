FROM 71620c7762c0

MAINTAINER Diogo Dutra <dutradda@gmail.com>
RUN apk update && apk add bash alpine-sdk lapack openblas
RUN ln -s /usr/include/locale.h /usr/include/xlocale.h
RUN git clone http://github.com/dutradda/myreco --branch moving_from_falconswagger_to_swaggerit /tmp/myreco
RUN pip install -r /tmp/myreco/requirements-dev.txt -r /tmp/myreco/requirements.txt --src /usr/local/src
RUN rm -rf /tmp/myreco
ADD run_tests.sh /usr/bin/run-tests
ENTRYPOINT /bin/bash
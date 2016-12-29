#!/bin/bash

cd /myreco
pip install -r requirements-dev.txt -r requirements.txt
py.test -c pytest-docker.ini $@

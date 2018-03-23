#!/bin/bash

export LANG=C.UTF-8
cd /myreco
tox -c tox-docker.ini

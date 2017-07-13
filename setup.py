# Copyright 2016 Diogo Dutra

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from version import VERSION
from setuptools import setup, find_packages

long_description = ''
with open('README.md') as readme:
    long_description = readme.read()

install_requires = None
with open('requirements.txt') as requirements:
    install_requires = requirements.readlines()

tests_require = None
with open('requirements-dev.txt') as requirements_dev:
    tests_require = requirements_dev.readlines()


setup(
    name='myreco',
    packages=find_packages('.'),
    include_package_data=True,
    version=VERSION,
    description='A Recommendations Framework',
    long_description=long_description,
    author='Diogo Dutra',
    author_email='dutradda@gmail.com',
    url='https://github.com/dutradda/myreco',
    download_url='http://github.com/dutradda/myreco/archive/master.tar.gz',
    license='MIT',
    keywords='recommendations neighborhood visualsimilarity visual similarity topseller top seller'\
    		' swagger openapi falconframework falcon framework',
    setup_requires=[
        'pytest-runner==2.9',
        'setuptools==28.3.0'
    ],
    tests_require=tests_require,
    install_requires=install_requires,
    classifiers=[
    	'License :: OSI Approved :: MIT License',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

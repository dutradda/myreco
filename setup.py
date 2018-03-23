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
from setuptools.command.test import test as TestCommand
import sys

long_description = ''
with open('README.md') as readme:
    long_description = readme.read()

install_requires = [
    'swaggerit',
    'numpy==1.*',
    'scipy==0.18.*',
    'boto3==1.*'
]

tests_require = [
    'pytest==3.*',
    'pytest-cov==2.*',
    'pytest-variables[hjson]==1.*',
    'pytest-aiohttp==0.1.*',
    'ipython==5.*',
    'PyMySQL==0.7.*'
]

setup_requires = [
    'pytest-runner',
    'flake8'
]


class PyTest(TestCommand):

    user_options = [
        ('cov-html=', None, 'Generate html report'),
        ('vars=', None, 'Pytest external variables file'),
        ('filter=', None, "Pytest setence to filter (see pytest '-k' option)"),
        ('path=', None, "The path to tests")
    ]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ['--cov', 'myreco', '-xvv']
        self.cov_html = False
        self.filter = False
        self.skip_unit = False
        self.vars = 'pytest-vars.json'
        self.path = 'tests/integration'

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.pytest_args.extend(['--variables', self.vars])

        if self.cov_html:
            self.pytest_args.extend(['--cov-report', 'html'])
        else:
            self.pytest_args.extend(['--cov-report', 'term-missing'])

        if self.filter:
            self.pytest_args.extend(['-k', self.filter])

        self.pytest_args.append(self.path)

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


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
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
    classifiers=[
    	'License :: OSI Approved :: MIT License',
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    cmdclass={'test': PyTest},
    test_suite='tests'
)

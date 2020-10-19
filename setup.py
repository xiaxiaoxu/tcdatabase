from setuptools import setup, find_packages

setup(
    name='tctools',
    version='0.1',
    packages=find_packages(),
    package_data={
        'tctools':["*.py"]
    },
    install_requires=['pymysql'],
    author = '__author__',
    author_email='__author__@jiguang.cn',
    url='https://github.com/jipush-qa-group/tctools.git',
)

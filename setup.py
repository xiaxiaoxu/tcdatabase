from setuptools import setup, find_packages

setup(
    name='tctools',
    version='0.1',
    packages=find_packages(),
    package_data={
        'tctools':["*.py"]
    },
    install_requires=['pymysql'],
    author = 'xiaxx',
    author_email='xiaxx@jiguang.cn',
    url='https://github.com/xiaxiaoxu/tctools.git',
)

#!/bin/bash

echo "Download Delta Core Dependencies"
sudo wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar -P /home/hadoop/

echo "Copy Delta Core Dependencies"
sudo cp /home/hadoop/delta-core_2.12-2.3.0.jar /lib/spark/jars/

echo "Download Delta Storage Dependencies"
sudo wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar -P /home/hadoop/

echo "Copy Delta Storage Dependencies"
sudo cp /home/hadoop/delta-storage-2.3.0.jar /lib/spark/jars/

sudo yum -y install python3-devel

sudo pip3 install boto3==1.26.90

sudo pip3 install psycopg2-binary==2.9.6

sudo pip3 install cython

sudo pip3 install pandas==1.3.5

sudo pip3 install seaborn==0.11.2

sudo pip3 install pyarrow

sudo pip3 uninstall numpy -y

sudo pip3 install numpy==1.17.3

sudo pip3 install pydantic==1.8.2

echo "Installing RedHat dependencies for psycopg2 (YUM)"
sudo yum install -y python3-devel postgresql postgresql-devel

echo "Installing libffi (YUM)"
sudo yum install -y gcc libffi-devel python-devel openSSL-devel

echo "Updating pip"
/usr/bin/pip3 install --user --upgrade pip

echo "Installing setuptools"
sudo yum install -y python3-setuptools

echo "Installing cython, numpy and pandas"
sudo pip3 install cython==0.29.24 numpy==1.21.3 pandas==1.2.4

echo "Installing scikit-learn"
sudo pip3 install scikit-learn

echo "Installing Psycopg2 (PIP)"
sudo pip3 install -U cryptography==3.3.2 pydantic==1.8.2 psycopg2==2.9.1 boto3==1.18.2 setuptools_rust==0.12.1 pysftp==0.2.9

echo "Installing requests"
sudo pip3 install requests

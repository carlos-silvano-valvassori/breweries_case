# breweries case
BEES Data Engineering

Objective:
The goal of this test is to assess your skills in consuming data from an API, transforming and
persisting it into a data lake following the medallion architecture with three layers: raw data,
curated data partitioned by location, and an analytical aggregated layer.

# How to run this project

Pre-requisites for running:
 * AWS Account 
 * AWS EMR Studio: Workspaces (Notebooks)
 * AWS EMR cluster 
	 * Delta Core Dependencies
	 * Python
	 * PySpark lib
	 * hdfs 
	 * EMR Applications: JupyterHub, Spark, JupyterEnterpriseGateway, hive


# Steps to run the medallion architecture in EMR cluster

CREATING A CLUSTER AND ADDING BRONZE, SILVER AND GOLD SERVICE IN STEPS
* Before creating the cluster:
	* Copy the script breweries.zip and main-data-eng.py into s3 path you have permission to copy it
	* copy the script from repo under src/breweries/common/resources/brewery_boostrap.sh to s3 path you have permission to copy it

* Create an EMR cluster (Amazon EMR version emr-6.10.1):
	* Name: Put a name that you will need to use for attaching in the emr workspace notebook later. Example of name: "abi.cluster.test"
	* with a Master Node and 2 Core nodes
	* Termination option: Manually terminate cluster
	* Installed applications: Spark 3.3.1, Hive 3.1.3, JupyterEnterpriseGateway 2.6.0, JupyterHub 1.5.0
	* Boostrap actions: add the bootstrap brewery_boostrap.sh 	
	* Copy the script breweries.zip and main-data-eng.py into s3 path you have permission to copy it
	* Add a new Step 1:
		* "Custom JAR"
		* "Name": Download Delta Core Dependencies
		* "Jar Location": command-runner.jar
		* "Arguments - optional":  sudo wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar -P /home/hadoop/
		* "Action if step fails": Continue
	* Add a new Step 2:
		* "Custom JAR"
		* "Name": Bronze Execution
		* "Jar Location": command-runner.jar
		* "Arguments - optional":  spark-submit --deploy-mode client --py-files s3://<your s3 path>/breweries.zip s3://<your s3 path>/main-data-eng.py --service ABIInbevBronzeService
		* "Action if step fails": Continue
	* Add a new Step 3:
		* "Custom JAR"
		* "Name": Bronze Execution
		* "Jar Location": command-runner.jar
		* "Arguments - optional":  spark-submit --deploy-mode client --py-files s3://<your s3 path>/breweries.zip s3://<your s3 path>/main-data-eng.py --service ABIInbevSilverService
		* "Action if step fails": Continue
	* Add a new Step 4:
		* "Custom JAR"
		* "Name": Bronze Execution
		* "Jar Location": command-runner.jar
		* "Arguments - optional":  spark-submit --deploy-mode client --py-files s3://<your s3 path>/breweries.zip s3://<your s3 path>/main-data-eng.py --service ABIInbevGoldService
		* "Action if step fails": Continue

	
# Steps to run the medallion architecture in EMR Jupyter Notebook
* This requires that a workspace creation first in "Amazon EMR" -> "EMR Studio: Workspaces (Notebooks)"
* After created the workspace, please take a note of the "Workspace ID" and "Workspace storage location" that is important to update the notebook test
* Execute a Quick Launch of your workspace in order to open the emr jupyter notebook session
* Attach the cluster created "EMR on EC2 cluster" in "EMR Compute" in left side
* Go to "File Browser" in left side and click on Upload files
* You will need to upload the file in repo called "abi_notebook.ipynb" from src/breweries/common/notebooks/abi_notebook.ipynb
* Now is just execute the steps in the notebook as example

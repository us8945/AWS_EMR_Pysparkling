# Installing H2O Sparkling Water on EMR - Python (Jupyter) environment




Table of Contents
=================

   * [Installing H2O Sparkling Water on EMR](#installing-h2o-sparkling-water-on-emr)
      * [Prerequisites](#prerequisites)
      * [Guide Purpose](#guide-purpose)
      * [Create and login to EMR Cluster](#create-and-login-to-emr-cluster)
      * [Installation Steps](#installation-steps)
         * [Step 1 - Install Python Anaconda Distribution](#step-1---install-python-anaconda-distribution)
         * [Step 2 - Install and configure Jupyter notebook/lab environment](#step-2---install-and-configure-jupyter-notebooklab-environment)
         * [Step 3 - Download and install H2O Sparkling Water](#step-3---download-and-install-h2o-sparkling-water)
         * [Step 4 - Jupyter notebook](#step-4---jupyter-notebook)
         * [Step 5 (Optional) - Start H2O in Scala Shell](#step-5-optional---start-h2o-in-scala-shell)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc)




## Prerequisites

This guide assumes Spark 2.4 is already provisioned with the EMR cluster. All tests have been performed using following SW versions:

1. EMR 5.23.0
2. Spark 2.4
3. H2O Sparkling Water 2.4.11
4. Anaconda 5.2.0 (Python 3.6.5)

You should be able to use the scripts and procedure unmodified as long as your EMR cluster has Spark 2.4 . You will need to make changes to the deployment script is the SW versions above change.

## Guide Purpose

This guide provides set of instructions to set up Python based environment to run H2O Sparling Water on AWS EMR (with Spark) cluster.

The Python run-time environment set up is automated and is based on Anaconda Python distribution. Once complete, the steps will provide following configuration:

1. Latest Anaconda Python 3.6 distribution. 
2. Jupyter Notebook/Lab set up and running under separate OS user
3. H2O Sparkling Water 2.4.11
4. Example of starter Notebook to demonstrate creating:
   1. Spark Session to connect to YARN cluser
   2. H2O context running side by side with Spark session



## Create and login to EMR Cluster

This is guide doesn't cover steps to provision EMR cluster. You could find many guides helping you to provision your first EMR cluster. When provisioning EMR cluster make sure you read "Prerequisites section" first. 



## Installation Steps 

All steps listed below are fully automated, and are part of the [aws_python_tools_install.py](https://github.com/us8945/AWS_EMR_Pysparkling/blob/master/set_up_scripts/aws_python_tools_install.py "Auto-install File") installation file.



### Step 1 - Install Python Anaconda Distribution

In this step we download and install Python Anaconda distribution. The distribution provides handy set of majority of the Python libraries. Many of the big organization certify Anaconda distribution for use on its company servers. 

### Step 2 - Install and configure Jupyter notebook/lab environment 

The goal of this step is to perform all necessary steps to create Jupyter notebook environment.  

1. Create OS user `jupyterlab`

2. The password for the user is created using random password generator. If you need to know the password, you will need to reset it using `sudo` access from `hadoop` OS user.

3. Define enviornment variables and place them into `.bash_profile`:

 ```
   # .bash_profile
   # Get the aliases and functions
   if [ -f ~/.bashrc ]; then
           . ~/.bashrc
   fi
   # User specific environment and startup programs
   PATH=$PATH:$HOME/.local/bin:$HOME/bin
   export PATH
   export PATH=/mnt/opt/anaconda520:/mnt/opt/anaconda520/bin:$PATH
   export PYSPARK_PYTHON=/mnt/opt/anaconda520/bin/python
   export SPARK_HOME=/usr/lib/spark
   export HADOOP_CONF_DIR=/etc/hadoop/conf
   export MASTER="yarn"
 ```

6. Initial token is generated when notebook starts for the first time. The token can be found by running following command: `sudo cat /home/jupyterlab/notebooks/jupyter_notebook.log | grep token`

7. The notebook is running using port `28888` . You can modify the port after restart. Following command is used to start the notebook:
``` 
sudo su - jupyterlab -c 'nohup jupyter notebook --no-browser --port 28888 >> /home/jupyterlab/notebooks/jupyter_notebook.log 2>&1 &'
```



### Step 3 - Download and install H2O Sparkling Water

Following steps are automated and part of the script.

```
mkdir /mnt/opt
cd /mnt/opt
wget https://s3.amazonaws.com/h2o-release/sparkling-water/rel-2.4/11/sparkling-water-2.4.11.zip
unzip sparkling-water-2.4.11.zip
```



### Step 4 - Jupyter notebook

The following steps are performed to validate working environment and ability to create Spark Session and H2O context from Jupyter notebook.

1. In this step we assume you have already opened port 28888 or using Browser pluging to route Web traffic to/from EMR

2. Connect to Jupyter notebook using EMR host name and port 28888. Optionally you can specify token all in one command. To find the info:

```
sudo cat /home/jupyterlab/notebooks/jupyter_notebook.log | grep
```
Example URL: http://ip-172-31-46-145:28888/tree?token=eede425006d001d6bdeb9d8afb2c910e87f3a8137ab38516

4. Upload provided notebook `H2O Sparkling Water EMR starter.ipynb` and repeat the steps. The notebook can be found under the `notebooks` folder of this Github repo.
5. The Starter notebook can be found here [H2O Sparkling Water EMR starter.ipynb](https://github.com/us8945/AWS_EMR_Pysparkling/blob/master/notebooks/H2O%20Sparkling%20Water%20EMR%20starter.ipynb "Starter Notebook")


### Step 5 (Optional) - Start H2O in Scala Shell

You can validate H2O Sparkling Water installation by running below commands and  `scala` shell.



```
export SPARK_HOME=/usr/lib/spark
export HADOOP_CONF_DIR=/etc/hadoop/conf
export MASTER="yarn"
   
bin/sparkling-shell \
--master yarn \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=1000000 \
--conf spark.ext.h2o.fail.on.unsupported.spark.param=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.autoBroadcastJoinThreshold=-1 \
--conf spark.locality.wait=30000 \
--conf spark.scheduler.minRegisteredResourcesRatio=1 \
--conf spark.executor.instances=4 \
--conf spark.executor.memory=2g \
--conf spark.driver.memory=2g 
```



The parameter `spark.ext.h2o.fail.on.unsupported.spark.param` is set because otherwise following error is raised on EMR: 

```
java.lang.IllegalArgumentException: Unsupported argument: (spark.dynamicAllocation.enabled,true)
```



Start H2O sessions in Scala shell

```
import org.apache.spark.h2o._
val h2oContext = H2OContext.getOrCreate(spark)
import h2oContext._
```



If successful following message is displayed:

```
scala> h2oContext
res0: org.apache.spark.h2o.H2OContext =

Sparkling Water Context:
 * H2O name: sparkling-water-hadoop_application_1558311857271_0004
 * cluster size: 3
 * list of used nodes:
  (executorId, host, port)
  ------------------------
  (2,ip-172-31-12-99.ec2.internal,54321)
  (1,ip-172-31-10-203.ec2.internal,54321)
  (3,ip-172-31-10-203.ec2.internal,54323)
  ------------------------

  Open H2O Flow in browser: http://ip-172-31-14-136.ec2.internal:54321 (CMD + click in Mac OSX)


 * Yarn App ID of Spark application: application_1558311857271_0004
```



Create sample DataFrame and validate from H2O UI:

```
val someDF = Seq(
  (8, "bat"),
  (64, "mouse"),
  (-27, "horse")
).toDF("number", "word")

someDF.show()
+------+-----+
|number| word|
+------+-----+
|     8|  bat|
|    64|mouse|
|   -27|horse|
+------+-----+


scala> val hfNamed: H2OFrame = h2oContext.asH2OFrame(someDF, Some("h2oframe"))
hfNamed: org.apache.spark.h2o.H2OFrame =
Frame key: h2oframe
   cols: 2
   rows: 3
 chunks: 3
   size: 487
```
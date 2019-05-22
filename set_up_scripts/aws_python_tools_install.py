'''
Python-2 script
Purpose:
- Install Anaconda Python3 distribution
- Install Pyspark
- Install Jupyter
- Install H2O Sparkling Water

Target Environment:
Release label:emr-5.23.0
Hadoop distribution:Amazon 2.8.5
Applications:Hive 2.3.4, Hue 4.3.0, Spark 2.4.0, JupyterHub 0.9.4

Example Port forwarding to access Jupyter notebook:
ssh -i /cygdrive/c/Users/your_user/.ssh/your_key_file.pem -L 28888:localhost:28888 hadoop@aws-host-name.compute.amazonaws.com


'''
import os
import sys
import subprocess
import hashlib
anaconda_md5 = "3e58f494ab9fbe12db4460dc152377b5"
anaconda_distro_url = "https://repo.continuum.io/archive/Anaconda3-5.2.0-Linux-x86_64.sh"
h2o_sparling_water_url = "https://s3.amazonaws.com/h2o-release/sparkling-water/rel-2.4/11/sparkling-water-2.4.11.zip"
sparkling_water_zip = "sparkling-water-2.4.11.zip"
anaconda_distro = "Anaconda3-5.2.0-Linux-x86_64.sh"
aws_install_location = "/mnt/opt"
install_location = os.path.join(aws_install_location,"anaconda520")
#Pyspark version must match Spark version!!!
pyspark_version = "2.4.0"

import random
import string
def randomStringDigits(stringLength=6):
    """Generate a random string of letters and digits """
    """ Source: https://pynative.com/python-generate-random-string/"""
    lettersAndDigits = string.ascii_letters + string.digits
    return ''.join(random.choice(lettersAndDigits) for i in range(stringLength))


def main():

	#Download Anaconda352 (Python 3.6.4)
	print("*******************************************************")
	print("Installing Anaconda:", anaconda_distro)
	print("*******************************************************")
	subprocess.call("sudo mkdir "+aws_install_location, shell="True")
	os.chdir(aws_install_location)
	subprocess.call("sudo wget "+ anaconda_distro_url,shell="True")
	#Check that MD5 hash matches one listed on distribution
	md5 = hashlib.md5(open(anaconda_distro,'rb').read()).hexdigest()
	if md5 != anaconda_md5:
		print('Invalid Anaconda MD5 hash. Check MD5 hash on Anaconda distro website and compare to anaconda_md5 parameter at the top')
		sys.exit(-1)
	
	# Run Anaconda installation
	subprocess.call("sudo bash "+anaconda_distro+" -b -p "+install_location, shell="True")
	
	
	#Install PySpark - Important: modify pyspark version to match version of Spark
	subprocess.call("sudo "+install_location+"/bin/conda install -y -q -c conda-forge pyspark="+pyspark_version, shell="True")
	
	#Install Hyperopt
	subprocess.call("sudo "+install_location+"/bin/conda install -y -q -c jaikumarm hyperopt", shell="True")
	
	
	
	# Install Jupyter
	print("*******************************************************")
	print("Install Jupyter")
	print("*******************************************************")
	subprocess.call("sudo "+install_location+"/bin/conda install -y -q -c conda-forge jupyterlab", shell="True")

	# Generate random password
	jupyterlab_password = randomStringDigits(10)
	 
	# Add Jupyter user to start notebook under the user
	subprocess.call("sudo useradd jupyterlab",shell="True")
	subprocess.call("sudo echo \""+jupyterlab_password+"\" | sudo passwd jupyterlab --stdin",shell="True")
	

	#create required folders in HDFS
	subprocess.call("hdfs dfs -mkdir /user/jupyterlab",shell="True")
	subprocess.call("hdfs dfs -chown -R jupyterlab:jupyterlab /user/jupyterlab", shell="True")
	subprocess.call("hdfs dfs -chmod 777 /user/jupyterlab",shell="True")
	
	
	# Create initial configuration:
	#   1) Allow accept connections from any ip
	#   2) Set working directory
	#   3) Copy starter notebooks
	#   4) Set authentication token
	subprocess.call("sudo -i -u jupyterlab mkdir notebooks",shell="True") 
	
	subprocess.call("sudo -i -u jupyterlab "+install_location+"/bin/jupyter notebook --generate-config", shell="True")
	
	subprocess.call("sudo echo '# Allow to accept connection from any IP' | sudo tee --append /home/jupyterlab/.jupyter/jupyter_notebook_config.py.new > /dev/null",shell="True")
	subprocess.call("sudo echo 'c.NotebookApp.ip ='\\''0.0.0.0'\\''  # listen on all interfaces' | sudo tee --append /home/jupyterlab/.jupyter/jupyter_notebook_config.py.new > /dev/null",shell="True")
	
	subprocess.call("sudo echo '# Set working directory' | sudo tee --append /home/jupyterlab/.jupyter/jupyter_notebook_config.py.new > /dev/null",shell="True")
	subprocess.call("sudo echo 'c.NotebookApp.notebook_dir = '\\''/home/jupyterlab/notebooks'\\'' # working folder' | sudo tee --append /home/jupyterlab/.jupyter/jupyter_notebook_config.py.new > /dev/null",shell="True")
	
	#Uncoment to set up with pre-configured token
	#subprocess.call("sudo echo '# Set Authentication token' | sudo tee --append /home/jupyterlab/.jupyter/jupyter_notebook_config.py.new > /dev/null",shell="True")
	#subprocess.call("sudo echo 'c.NotebookApp.token = '\\''6e1b6dc99965d0b2db443c55752fe7ce5d9079470284c960'\\'' # working folder' | sudo tee --append /home/jupyterlab/.jupyter/jupyter_notebook_config.py.new > /dev/null",shell="True")
	
	subprocess.call("sudo cat  /home/jupyterlab/.jupyter/jupyter_notebook_config.py | sudo tee --append /home/jupyterlab/.jupyter/jupyter_notebook_config.py.new > /dev/null",shell="True")
	subprocess.call("sudo mv /home/jupyterlab/.jupyter/jupyter_notebook_config.py.new /home/jupyterlab/.jupyter/jupyter_notebook_config.py", shell="True")
	
	# Add Anaconda to .bash_profile of jupyter user
	subprocess.call("sudo echo 'export PATH="+install_location+":"+install_location+"/bin:$PATH' | sudo tee --append /home/jupyterlab/.bash_profile > /dev/null",shell="True")
	
	# Add PySpark and H2O environment variables
	subprocess.call("sudo echo 'export PYSPARK_PYTHON="+install_location+"/bin/python' | sudo tee --append /home/jupyterlab/.bash_profile > /dev/null",shell="True")
	subprocess.call("sudo echo 'export SPARK_HOME=/usr/lib/spark' | sudo tee --append /home/jupyterlab/.bash_profile > /dev/null",shell="True")
	subprocess.call("sudo echo 'export HADOOP_CONF_DIR=/etc/hadoop/conf' | sudo tee --append /home/jupyterlab/.bash_profile > /dev/null",shell="True")
	subprocess.call("sudo echo 'export MASTER=\"yarn\"' | sudo tee --append /home/jupyterlab/.bash_profile > /dev/null",shell="True")
	
	# Start Jupyter notebook
	subprocess.call("sudo su - jupyterlab -c 'nohup jupyter notebook --no-browser --port 28888 >> /home/jupyterlab/notebooks/jupyter_notebook.log 2>&1 &'",shell="True")
	
	# Start Jupyter Lab
	#subprocess.call("sudo su - jupyterlab -c 'nohup jupyter lab --no-browser --port 18889 >> /home/jupyterlab/notebooks/jupyter_lab.log 2>&1 &'",shell="True")
	
	# Install H2O Sparkling Water and Python dependencies
	os.chdir('/mnt/opt')
	subprocess.call("sudo wget "+ h2o_sparling_water_url,shell="True")
	subprocess.call("sudo unzip "+ sparkling_water_zip, shell="True")
	subprocess.call("sudo "+install_location+"/bin/conda install -y -q requests", shell="True")
	subprocess.call("sudo "+install_location+"/bin/conda install -y -q tabulate", shell="True")
	subprocess.call("sudo "+install_location+"/bin/conda install -y -q future", shell="True")
	subprocess.call("sudo "+install_location+"/bin/conda install -y -q colorama\>\=0.3.8", shell="True")
	
	#Cleanup
	print("Cleaning up Anaconda shell file")
	subprocess.call("sudo rm "+anaconda_distro,shell="True")
	
	# Change mode on created folders/files to 755
	subprocess.call("sudo chmod 755 -R *",shell="True")
	
	print("Complete anaconda_python3 script run")
	
if __name__=="__main__":
	main()
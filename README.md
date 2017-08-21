# alzheimers-diagnosis-mapreduce
A computational system that analyzes Alzheimer's Disease diagnoses using the gene expression profiles of patients. It uses MapReduce for feature engineering and Spark for gene clustering and classification. By [Ada Chen](https://github.com/adachen), [William Wu](https://github.com/willgocode), and [myself](https://github.com/eakelly) for our Big Data class.


## Table of Contents
 - [Requirements](#requirements)
   - [HDFS Setup](#hdfs-setup)
   - [Spark Setup](#spark-setup)
   - [Running the Program](#running-the-program)
 - [Running with AWS](#running-with-aws)
 - [File List](#file-list)

## Requirements

### HDFS Setup

Make sure that you have [Homebrew](https://brew.sh) installed.

1. Install [Hadoop](https://hadoop.apache.org) with Homebrew by typing the following in a terminal window.
 ```none
 brew install hadoop
 ```

2. Configure your ```.bashrc``` file to include:
 ```none
 export HADOOP_HOME=/usr/local/Cellar/hadoop/2.8.0/libexec
 export HADOOP_MAPRED_HOME=$HADOOP_HOME
 export HADOOP_COMMON_HOME=$HADOOP_HOME
 export HADOOP_HDFS_HOME=$HADOOP_HOME
 export YARN_HOME=$HADOOP_HOME
 export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
 ```

3. Source your ```.bashrc``` file by doing the following:
 ```none
 source .bashrc
 ```

4. Change directory by doing ```cd /usr/local/Cellar/hadoop/2.8.0/libexec/etc/hadoop``` and configure the following files:
 
 1. **hdfs-site.xml**

 ```none
 <configuration>
  <property>
	  <name>dfs.replication</name>
	  <value>1</value>
  </property>
 </configuration>
 ```

 2. **core-site.xml**

 ```none
 <configuration>
  <property> 
	   <name>hadoop.tmp.dir</name>
	   <value>/usr/local/Cellar/hadoop/hdfs/tmp</value>
	   <description> A base for other temporary directories. </description>
  </property>
   . . .
  <property>
	  <name>fs.default.name</name>
	  <value>hdfs://localhost:9000</value>
  </property>
 </configuration>
 ```
5. Next, format the hdfs master node by typing the following in a terminal window. 

 ```none
 hdfs namenode -format
 ```
 
6. hdfs dfs -mkdir -p ~/<directory name>
	e.g. hdfs dfs -mkdir inputfiles

	CHECK
	hdfs dfs -ls hdfs://localhost:9000/user/…/<new directory name>

	e.g. hdfs dfs -ls hdfs://localhost:9000/user/Elizabeth/inputfiles

7. hdfs dfs -put ~/<file location path on computer> <directory name>/<file name>
	where <file name> = ROSMAP_RNASeq_entrez.csv, gene_cluster.csv
	
	e.g. hdfs dfs -put ~/Documents/BigData/Project2/ROSMAP_RNASeq_entrez.csv 	    inputfiles/ROSMAP_RNASeq_entrez.csv
		 hdfs dfs -put ~/Documents/BigData/Project2/gene_cluster.csv inputfiles/		 gene_cluster.csv

8. hdfs namenode
9. hdfs datanode (in a new terminal window)

10. In the .py file, change the file paths to:
	file1 = “hdfs://localhost:9000/user/…/<directory name>/ROSMAP_RNASeq_entrez.csv”
	file2 = “hdfs://localhost:9000/user/…/<directory name>/gene_cluster.csv”

	e.g.
		file_rosmap = "hdfs://localhost:9000/user/Elizabeth/inputfiles/ROSMAP_RNASeq_entrez.csv"
		file_gene_cluster = "hdfs://localhost:9000/user/Elizabeth/inputfiles/gene_cluster.csv"

 

### Spark Setup

### Running the Program

## Running with AWS

## File List

Prerequisites for running the application:
1)	Download a copy of RTKLIB from github:

# git clone https://github.com/tomojitakasu/RTKLIB

2)	Compile detect_gnss_timestamp
A Makefile has been provided with detect_gnss_timestamp that allows the tool to be built. The path to the local copy of RTKLIB need to be specified as argument:

# make DIR=/usr/src/RTKLIB 

3)	Install Python modules required for communication with Hive and HDFS:
On a Centos 7 system the following commands needs to be executed to install module prerequisites:
# yum -y install cyrus-sasl-devel cyrus-sasl-plain
The next step is to install the modules:
#pip install sasl
#pip install thrift
#pip install thrift-sasl
#pip install pyhs2
#pip install hdfs 

4)	Edit the address of Hive and the HDFS NameNode in automation_tool.py:
hive_host="master1"
hdfs_host="master1"

The automation_tool does not require to be run inside of Hadoop cluster, however it requires access to tcp port 50070 (NameNode) and 10000 (Hive).

5)	Run the Hadoop Algorithm from the Hadoop cluster on a server with the Spark client installed. 

The following steps are required to run the application:
1)	Obtain the GNSS files: The station files have to be downloaded on the local disk. Depending on the selected processing profile additional files may be required. A detailed list of supported files is presented in section 3.2.1 (Review of the current GNSS implementation)
2)	Run the Automation Tool and pass the GNSS files as arguments:
	# python automation_tool.py GNSS_FILE1 GNSS_FILE2 ….
The Automation Tool will invoke detect_gnss_timestamp and upload the files to HDFS and their metadata to Hive table gnss_files.
3)	Run the Hadoop Algorithm with the rnx2rtkp configuration files as argument. 
 Currently only PPP, SPP and eSPP profiles are supported.
# spark-submit --files /etc/spark2/conf/hive-site.xml --master yarn-cluster --num-executors 16 algorithm.py PPP.conf SPP.conf eSPP.conf
This will process all the station files from the gnss_files table and save processed data in “processed” Hive table.
The file /etc/spark2/conf/hive-site.xml has the information of Hive metastore and is required in order to be able to access the Hive tables.


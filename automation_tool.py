# duru
import sys
import os
import re
import traceback
import time
import datetime
import string
import subprocess
import pyhs2
from hdfs import Config
import hdfs

#pip install sasl
#pip install thrift
#pip install thrift-sasl
# pip install pyhs2

force_update = 0
hive_host="master1"
hdfs_host="master1"
hdfs_path = "/tmp/gnss/"

inserts = []

time_hdfs = 0
time_uncompress = 0
time_detect = 0
time_query = 0
time_insert = 0

client = hdfs.InsecureClient('http://%s:50070' % hdfs_host, user='root')

def get_ts():
	now = datetime.datetime.now()
	seconds = time.mktime(now.timetuple())
	seconds += now.microsecond/ 1000000.0
	return seconds

def upload_file(local_path, hdfs_file):
		global time_hdfs, client
		now = get_ts()
		output = 0
		try:
			client.upload(hdfs_file, local_path)
		except:
			print "\nException : %s \n" % (string.join( apply( traceback.format_exception, sys.exc_info() ), ""))
			output = 1
		time_hdfs += (get_ts() - now)
		return output


def add_file_to_db(cur, file_name):
	try:
		global inserts, time_uncompress, time_detect, time_query, time_insert
		now = get_ts()
		file_name_no_path = file_name.split('/')[-1]
		if file_name[-2:] == '.Z':
			command = 'cat %s | gunzip > /tmp/output' % (file_name)
			print "Running command: %s" % command
			subprocess.check_call(command, shell=True)
			file_name = "/tmp/output"
			file_name_no_path = file_name_no_path[:-2]
		time_uncompress += (get_ts() - now)
		#Obtain the GNSS file information
		now = get_ts()
		output = subprocess.check_output(["detect_gnss_timestamp", file_name]).split()
		time_detect += (get_ts() - now)

		print "got %s" % output
		if output[0] not in ("rinex", "clk", "sp3", "ionex", "obs"):
			print "The file %s cannot be processed by RTKLIB " % file_name
		(type, start, end) = (output[0], output[1], output[2])

		#check if the file exists already in the database
		query = "select name,path from gnss_files where name='%s' and type='%s' and time_start=%s and time_end=%s" % (file_name_no_path,type, start, end)
		print "Running the query: %s" % query
		now = get_ts()
		cur.execute(query)
		result = cur.fetchone()
		time_query += (get_ts() - now)

		if result is not None:
			print "Row already exist in the table --> " + result[1]
			if(force_update == 0):
				return result[1]

		#compute the HDFS file name in format FILENAME_STARTTIME_ENDTIME.EXTENSION
		fn = file_name_no_path.rsplit('.', 1)
		if(len(fn) < 2):
			print "File %s does not have an extension and it cannot be added to the database" % file_name
		hdfs_file = hdfs_path + fn[0] + "_" + start + "_" + end + "." + fn[1]

		#upload the file to HDFS
		print "File name from HDFS: " + hdfs_file
		output = upload_file(file_name, hdfs_file)
		if output == 0:
			print "The file was uploaded successfully to HDFS"
		else:
			print "File already uploaded to HDFS"

			#insert the record in the Hive table
		inserts.append("('%s','%s','%s',%s,%s)"  % (file_name_no_path,type,hdfs_file,start,end))

		return hdfs_file
	except:
		print "could not add file %s to table" %  file_name
		print "\nException : %s \n" % (string.join( apply( traceback.format_exception, sys.exc_info() ), ""))
		return None
#take a list of rovers as argument, adds them to the gnss_files and prepares the jobs in rovers table
#def create_job_entry(rovers):

#	for rover in rovers:


try:
	conn = pyhs2.connect(host=hive_host, port=10000, authMechanism="PLAIN", user='root', password='test', database='default')
	cur = conn.cursor()

	cur.execute("""create table if not exists gnss_files (
  		name		string,
		type		string,
		path		string,
		time_start	int,
		time_end	int
		)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
	""");
#		CLUSTERED BY (time_start) INTO 2 BUCKETS STORED AS ORC tblproperties ('orc.compress' = 'SNAPPY', 'transactional'='true')
except:
	print "could not connect to Hive " +  hive_host
	print "\nException : %s \n" % (string.join( apply( traceback.format_exception, sys.exc_info() ), ""))
cur.execute('select name,type,path,from_unixtime(time_start) as time_start,from_unixtime(time_end) as time_end from gnss_files');
#cur.execute('select name,type,path,time_start,time_end from gnss_files');
i = cur.getSchema()
print "-"*136
print "| %20s | %10s | %50s | %20s | %20s |" % (i[0]['columnName'],i[1]['columnName'],i[2]['columnName'],i[3]['columnName'],i[4]['columnName'])
print "-"*136
for i in cur.fetch():
    print "| %20s | %10s | %50s | %20s | %20s |" % (i[0],i[1],i[2],i[3],i[4])
print "-"*136

for i in range(1, len(sys.argv)):
	add_file_to_db(cur, sys.argv[i])

if len(inserts) > 0:
	now = get_ts()
	query = "insert into gnss_files values %s" % ",".join(inserts)
	print "SQL insert query (%d): %s " % (len(inserts), query)
	cur.execute(query)
	inserts = []
	time_insert += (get_ts() - now)


print "Time to upload files in HDFS: %.2f s" % time_hdfs
print "Time to uncompress the files: %.2f s" % time_uncompress
print "Time to detect timestamp: %.2f s" % time_detect
print "Time to run the Hive query: %.2f s" % time_query
print "Time to insert the metadata in Hive: %.2f s" % time_insert

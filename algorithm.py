from os.path import expanduser, join
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
import requests
import datetime
import pprint
import re
import subprocess
import time
import math

spark = SparkSession \
    .builder \
    .appName("GNSS Processing by Darina Toda") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
process_time = sc.accumulator(0)
process_number = sc.accumulator(0)

configs={}

for i in sys.argv:
	if i[-5:] == ".conf":
		configs[i] = ""

#configs={"242p11_eSPP.conf":"", "242p11_PPP.conf":"", "242p11_SPP.conf":""}

#configs={"242p11_eSPP.conf":""}

def log(s):
    print "%s %s" % (datetime.datetime.now().strftime('%H:%M:%S'), s)

def write_to_file(filename, content):
	f = open(filename, "wb")
	f.write(content)
	f.close()

list_of_local_files=["/root/gnss/rnx2rtkp_242p11"]

def add_local_files(content):
    for i in ["file-satantfile","file-rcvantfile","file-dcbfile"]:
        fn = re.findall("%s[ ]*=(.*)" % i, content)
        if len(fn) > 0 and os.access(fn[0].strip(), os.F_OK):
            filename = fn[0].strip()
            new_file = filename.split('/')[-1]
            list_of_local_files.append(filename)
            filename = filename.split('/')[-1]
            content = re.sub("%s.*" % i, "%s=%s" % (i,new_file) , content)
    return content


def map_config(x):
    result = []
    for i in configs:
        result.append((x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],i))
    return iter(result)

def download_file(hdfs_path, local_path):
	r=requests.get('http://master1:50070/webhdfs/v1%s?op=OPEN' % hdfs_path)
	write_to_file(local_path, r.content)

def compute_gnss(x):
    dir = "xx/"
    tmp = ""
    try:
        os.mkdir(dir)
    except:
        tmp="dir already exist,"

    files=[]
    for i in range(1,6):
        file = x[i]
        fn = file.split('/')[-1]
        fn = dir+fn
        if not os.access(fn, os.W_OK):
            download_file(file, fn)
        files.append(fn)
    (nav_path, obs_path, clk_path, sp3_path, ionex_path) = files
    time_start = datetime.datetime.fromtimestamp(x[6]).strftime('%Y/%m/%d %H:%M:%S')
    time_end = datetime.datetime.fromtimestamp(x[7]).strftime('%Y/%m/%d %H:%M:%S')
    initial_config_file = x[8]
    name = x[0]
    output = dir+name[0:-4]+"_"+initial_config_file[0:-5]+".pos"

    #replace file-ionofile with accurate location
    conf = configs[initial_config_file]
    conf = re.sub(r"file-ionofile.*","file-ionofile=%s" % ionex_path,conf)
    config_file = dir+initial_config_file
    #write config file to the disk
    write_to_file(config_file, conf)

    gnss_job = ["./rnx2rtkp_242p11", "-k",config_file,"-ts",time_start,"-te", time_end,"-o", output,obs_path, nav_path, clk_path, sp3_path, ionex_path]
    log("Starting on %s: %s" % (os.uname()[1], ' '.join(gnss_job)))

    proc_output=""
    now = time.time()
    try:
        proc_output = subprocess.check_output(gnss_job, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError, e:
        proc_output = ' '.join(gnss_job) + '\n'
        proc_output += e.output
        proc_output += " " + os.getcwd() +'\n'
        proc_output += os.popen("ls -al").read()
        subprocess.check_output(["/bin/rm", "-rf", dir], stderr=subprocess.STDOUT)
        return proc_output.split('\n')
    job_time = time.time() - now
    log("SUCCESS %s running time %d sec" % (name, job_time))
    process_number.add(1)
    process_time.add(job_time)
#    print proc_output
    fh = open(output)
    pos_file=fh.read().split("\n")
    fh.close()
    subprocess.check_output(["/bin/rm", "-rf", dir], stderr=subprocess.STDOUT)
    return_value = []
    for line in pos_file:
        if len(line)> 1 and line[0] != '%':
            newline = line.strip().split()
            if len(newline) == 15:
                newline[1] = math.trunc(time.mktime(datetime.datetime.strptime(newline[0]+ " " + newline[1], "%Y/%m/%d %H:%M:%S.%f").timetuple()))
                newline[0] = name[0:-4]+"_"+initial_config_file
                for i in range(2,15):
                    newline[i] = float(newline[i])
                return_value.append(newline)
            else:
                log("invalid line -> %s" % (line))
                print newline

    return return_value



log("Start")
now =  time.time()

cache_before = time.time()
table = "gnss_files"
query = "select clocks.path as clk_path, sp3.path as sp3_path, ionex.path as ionex_path, ionex.time_start as sat_time_start, ionex.time_end as sat_time_end from %s clocks, %s sp3, %s ionex where sp3.type = 'sp3' and clocks.type = 'clk' and ionex.type = 'ionex' and sp3.time_start = clocks.time_start and clocks.time_start = ionex.time_start " % (table,table,table)
sat_ionex = spark.sql(query).cache()

#query = "select nav.path as nav_path, obs.path as obs_path, greatest(obs.time_start, nav.time_start) as rover_time_start, least(nav.time_end, obs.time_end) as rover_time_end from %s nav, %s obs where nav.type = 'rinex' and obs.type='obs' and obs.time_start<=nav.time_start AND obs.time_end<=nav.time_end AND SUBSTRING(obs.name, 0, LENGTH (obs.name)-4)= SUBSTRING(nav.name, 0, LENGTH (nav.name)-4)" % (table, table)
query = "select obs.name as name,nav.path as nav_path, obs.path as obs_path, greatest(obs.time_start, nav.time_start) as rover_time_start, least(nav.time_end, obs.time_end) as rover_time_end from %s nav, %s obs where nav.type = 'rinex' and obs.type='obs' and obs.time_start>=nav.time_start AND abs(obs.time_end - nav.time_end) < 3600" % (table, table)

rover = spark.sql(query) #.cache()

#cache the tables in the memory to improve the pero
print "We have %d different satellite and ionospehere files " % sat_ionex.count()

processing = rover.join(sat_ionex, (rover.rover_time_start >= sat_ionex.sat_time_start) & (rover.rover_time_end <= sat_ionex.sat_time_end)).select("name","nav_path", "obs_path", "clk_path", "sp3_path", "ionex_path", "rover_time_start","rover_time_end").cache()

print "The final table has %d records" % rover.count()

cache_after = time.time()


for i in configs:
    log("Reading config file %s " % i)
    f = open("/root/gnss/"+i)
    config=f.read()
    f.close()
    configs[i] = add_local_files(config)

for i in set(list_of_local_files):
    log("Adding file %s to the list of resources" % (i))
    sc.addFile(i)

rdd = processing.rdd.map(tuple)

log("files -> %d" % (rdd.count()))
obs_config = rdd.flatMap(map_config)
records = obs_config.count()
log("Records -> %d" % (records))
partitions = int(sc.getConf().get("spark.executor.instances"))
if records > 0 and (records / 20 + 1) < partitions:
    partitions = (records / 20 + 1)

obs_config = obs_config.coalesce(partitions, True)

log("Records: %d, partitions %d" % (records, obs_config.getNumPartitions()))
before_compute = time.time()
processed = obs_config.flatMap(compute_gnss)
log("Total number of processed observation: %d" % (processed.cache().count()))
log("Total rnx2rtkp processes: %d" % process_number.value)
after_compute = time.time()

df = processed.toDF(['name', 'Timestamp','latitude', 'longitude', 'height', 'Q', 'ns', 'sdn_m', 'sde_m','sdu_m', 'sdne_m','sdeu_m','sdun_m','age_s','ratio'])
log("Created Data Frame")

output_table = "results"
log("Writing the results to Hive table %s" % output_table)
before_save = time.time()

df.write.format('orc').mode('overwrite').saveAsTable(output_table)
after_save = time.time()

log("Table saved")

log("Querying the table to identify the required files took %d seconds" % (cache_after - cache_before))
log("Processing GNSS data took %d seconds" % (after_compute - before_compute))
log("Total rnx2rtkp processes: %d" % process_number.value)
log("Total seconds spend by rnx2rtkp %d, speedup %.2f" % (process_time.value, 1.0*process_time.value/ (after_compute - before_compute)))

log("Saving to Hive table took %d seconds" % (after_save - before_save))

log("Total time %d seconds" % (time.time() - now))

# ============================================================
# Author: Rochak Wadhwa
# Purpose of this sctipt: Clean logs and not needed old files from local of HDP node
# This script can be executed using root user
# Version - Oct 16
# ============================================================



# ============================================================
# importing modules and utilities
import os
import time
import getpass as gt
import stat



# ============================================================
# List for deletion of log files ending like out.1 or log.7
loc01 = "/var/log/ambari-agent/"
loc02 = "/var/log/ambari-infra-solr/"
loc03 = "/var/log/ambari-metrics-collector/"
loc04 = "/var/log/ambari-metrics-monitor/"
loc05 = "/var/log/ambari-server/"
loc06 = "/var/log/atlas/"
loc07 = "/var/log/audit/"
loc08 = "/var/log/hadoop/hdfs/"
loc09 = "/var/log/hadoop-mapreduce/mapred/"
loc10 = "/var/log/hadoop-yarn/embedded-yarn-ats-hbase/"
loc11 = "/var/log/hadoop-yarn/yarn/"
loc12 = "/var/log/hbase/"
loc13 = "/var/log/hst/"
loc14 = "/var/log/hive/"
loc15 = "/var/log/smartsense-activity/"
loc16 = "/var/log/spark2/"
loc17 = "/var/log/zookeeper/"

local_locations = [loc01, loc02, loc03, loc04, loc05, loc06, loc07, loc08, loc09, loc10, loc11, loc12, loc13, loc14, loc14, loc15, loc16, loc17]


def deleteDaysOldLogsWithPattern():
	print(" ")
	deleteDaysOldDefinition("/var/log/",50,".log-202","log","202")
	deleteDaysOldDefinition("/var/log/ambari-metrics-collector/",50,"gc.log-202","202",".log-")
	deleteDaysOldDefinition("/var/log/atlas/",50,"audit.log","audit","202")
	deleteDaysOldDefinition("/var/log/aws/codedeploy-agent/",50,"codedeploy-agent.202",".log",".202")
	deleteDaysOldDefinition("/var/log/hadoop/hdfs/",50,"gc.log-202","gc.log","-202")
	deleteDaysOldDefinition("/var/log/hadoop/hdfs/audit/solr/spool/archive/",50,"202","spool_hdfs","log")
	deleteDaysOldDefinition("/var/log/hadoop/hdfs/audit/hdfs/spool/archive/",50,"202","spool_hdfs","log")
	deleteDaysOldDefinition("/var/log/hadoop/hdfs/",50,"gc.log-202","202","gc")
	deleteDaysOldDefinition("/var/log/hadoop/hdfs/",50,"hdfs-audit.log.202","202",".log")
	deleteDaysOldDefinition("/var/log/hadoop-yarn/yarn/",50,"nm-audit.log.202","202",".log.")
	deleteDaysOldDefinition("/var/log/hadoop-yarn/yarn/",50,"rm-audit.log.202","202",".log.")
	deleteDaysOldDefinition("/var/log/hbase/",50,"gc.log-202","gc.log","202")
	deleteDaysOldDefinition("/var/log/hbase/audit/hdfs/spool/",50,"spool_hbaseRegional_202","spool","202")
	deleteDaysOldDefinition("/var/log/hbase/audit/hdfs/spool/",50,"spool_hbaseRegional_202","202",".log")
	deleteDaysOldDefinition("/var/log/hive/",50,"hive","202",".current")
	deleteDaysOldDefinition("/var/log/kafka/",50,"log-cleaner.log.202","202","log")
	deleteDaysOldDefinition("/var/log/ranger/kms/",50,"kms.log.202",".log","202")
	deleteDaysOldDefinition("/var/log/knox/",50,"gateway",".log.202",".log.")
	deleteDaysOldDefinition("/var/log/nifi-registry/",50,".log.202",".202","log.")
	deleteDaysOldDefinition("/var/log/nifi-registry/",50,"202",".log","nifi-registry-")
	deleteDaysOldDefinition("/var/log/ranger/admin/",50,"xa_portal.log.202","log","202")
	deleteDaysOldDefinition("/var/log/ranger/admin/",50,"access_log.202","log","202")
	deleteDaysOldDefinition("/var/log/ranger/kms/",50,"access_log.202","log","202")
	deleteDaysOldDefinition("/var/log/ranger/tagsync/",50,"tagsync.log.202","log","202")
	deleteDaysOldDefinition("/var/log/ranger/usersync/",50,".log.202","202",".log.")

	

	
def deleteMoreLogs():
	print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")
	print("Deleting old log files from specific locations, if exist")
	time.sleep(2)
	print("---- Checking and deleting old Hive logs")
	os.system("rm -rf /var/log/hive/hive*202*.gz")
	time.sleep(2)
	print("---- Checking and deleting old kafka Logs")
	os.system("rm -rf /var/log/kafka/controller.log.202*")
	os.system("rm -rf /var/log/kafka/server.log.202*")
	time.sleep(2)
	print("---- Checking and deleting old nifi logs")
	os.system("rm -rf /var/log/nifi/nifi-bootstrap_202*")
	os.system("rm -rf /var/log/nifi/nifi-app_202*")
	os.system("rm -rf /var/log/nifi/nifi-user_202*")
	time.sleep(2)
	print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")
	print(" ")
	print(" ")
	time.sleep(2)

# ============================================================
# defining functions to be used in main section



def deleteLogOutDotNumber(local_locations):
	for location in local_locations:
		if os.path.isdir(location):
			print(" ")
			print("---- ----- ----- -----")
			time.sleep(0.2)
			print("Checking " + location)
			files = [ f for f in os.listdir(location) if os.path.isfile(os.path.join(location,f)) ]
			flag = "false"
			for f in files:
				if (f.count('.') > 1):
					if ((f.split('.')[-2] in ["out","log","err"]) and f.split('.')[-1].isdigit()):
						print("Deleting " + location + f)
						os.system("rm -rf " + location + f)
						time.sleep(0.2)
						flag = "true"

			if flag == "false":
				time.sleep(0.2)
				print("No old file found at this location")
				time.sleep(0.5)


def deleteDaysOldDefinition(location,n,pattern1,pattern2,pattern3):

	if os.path.isdir(location):

		print(" ")

		print("----- ----- ---- -----")

		#print(" ")

		time.sleep(0.3)

		print("Checking " + location)

		files = [ f for f in os.listdir(location) if os.path.isfile(os.path.join(location,f)) ]

		for f in files:

			if pattern1 in f and pattern2 in f and pattern3 in f:

				FileFullPath = location + f

				#print("Processing " + FileFullPath)

				fileStats = os.stat(FileFullPath)

				last_access = fileStats[stat.ST_ATIME]

				now = time.time()

				days = (now - last_access) / (60 * 60 * 24)

				if days > n :

					if "log" in FileFullPath and "202" in FileFullPath:

						print("Deleting " + FileFullPath + " -- last accessed: " + str(int(days)) + " days before")

						os.system("rm -rf " + FileFullPath)

						time.sleep(0.1)





if __name__ == '__main__':

	try:

		print(" ")

		print(" ")

		user = gt.getuser()

		rootUser = 'root'

		if user == rootUser:

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")



			time.sleep(0.5)

			print('Deleting old logs from various locations')

			time.sleep(0.5)

			print(" ")

			print(" ")

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			print("Checking locations for files like .log.1 or .err.7 etc")

			deleteLogOutDotNumber(local_locations)

			time.sleep(0.5)

			print(" ")

			print(" ")

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			time.sleep(0.5)

			print("Checking location for old files and not touched recently")

			deleteDaysOldLogsWithPattern()

			print(" ")

			#print(" ")

			deleteMoreLogs()

			time.sleep(0.5)

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")



			time.sleep(0.5)

			print(" ")

			time.sleep(1)

			print('Logs from various locations have been deleted')

			time.sleep(1)

			print(" ")

			print(" ")

			print(" ")

		else:

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			print('Please use this script with root user')

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			



	except Exception as error:

		print(error)

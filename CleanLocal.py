# ============================================================
# Author: Rochak Wadhwa
# Team: Data Engineering under Vinod Boga
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
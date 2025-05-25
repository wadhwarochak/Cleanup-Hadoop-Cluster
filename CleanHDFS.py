# ============================================================
# Author: Rochak Wadhwa
# Team: Data Engineering under Vinod Boga
# Purpose of this sctipt: Clean logs and not needed old files from HDFS of HDP node
# This script can be executed using hdfs user
# ============================================================



import os
import time
import getpass as gt
import subprocess
from datetime import datetime
now = datetime.now()
from dateutil.relativedelta import relativedelta
import calendar
import datetime as dt
from datetime import date


def clearAppLogs():

        print(" ")

        print(" ")

        print("--- --- --- --- --- --- --- --- --- --- --- ---")

        print("Clearing old logs from app-logs")

        print(" ")

        hdfsdir = '/app-logs/'

        try:

                proc = subprocess.Popen(["hadoop fs -ls " + hdfsdir + "| tr -s ' ' | cut -d' ' -f8"], stdout=subprocess.PIPE, shell=True)

                (out, err) = proc.communicate()

                for byte_line in out.splitlines():

                        path = byte_line.decode()

                        #time.sleep(0.3)

                        if path:

                                path = path + "/logs/application_*"

                                #print(" ")

                                #print("--- --- --- --- --- --- --- --- --- --- --- ---")

                                #print("Deleting files at " + path)

                                os.system("hdfs dfs -rm -R -skipTrash " + path)

        except Exception as error:

                print(error)





def remove_old_files_hive_llap_tar_gz():

        print(" ")

        print(" ")

        print("--- --- --- --- --- --- --- --- --- --- --- ---")

        print("Clearing old hive llap")

        n=3

        hive_llap = '/user/hive/.yarn/package/LLAP/'

        given_date = str(datetime.now())

        dtObj = datetime.strptime(given_date[0:10], '%Y-%m-%d')

        past_date = dtObj - relativedelta(months=n)

        past_date_str = past_date.strftime('%Y-%m-%d')

        year = past_date_str[0:4]

        month = past_date_str[5:7]

        day = past_date_str[8:10]

        date = dt.date(int(year),int(month),int(day))

        try:

                #print(" ")

                #print(" ")

                #print(" ")

                time.sleep(0.5)



                PurgeBeforeDate = (date.replace(day = calendar.monthrange(date.year, date.month)[1])).strftime("%Y-%m-%d")

                print("Checking files before " + PurgeBeforeDate + " under " + hive_llap)

                proc = subprocess.Popen(["hadoop fs -ls " + hive_llap + "| tr -s ' ' | cut -d' ' -f6,8"], stdout=subprocess.PIPE, shell=True)

                (out, err) = proc.communicate()

                for byte_line in out.splitlines():

                        line = byte_line.decode()

                        if line:

                                if (line.split()[0] < PurgeBeforeDate):

                                	#time.sleep(1)

                                	command = "hdfs dfs -rm -R -skipTrash " + line.split()[1]

                                	if "/user/hive/.yarn/package/LLAP/" in command:

                                        	if ".tar.gz" in command:

                                                	#print(command + " ---- " + line.split()[0])

                                                	os.system(command)

                time.sleep(0.5)

                print(" ")

                #print(" ")

                #print(" ")

                #print("Old LLAP logs have been cleared")

                time.sleep(0.5)

                #print("===== ===== ===== ===== =====")          

                #print(" ")

                print(" ")

        except Exception as error:

                print(error)







def PurgeRanger():

        print(" ")

        print(" ")

        print("--- --- --- --- --- --- --- --- --- --- --- ---")

        print("Deleting old folders from /ranger/audit/hdfs/")

        for i in range(3, 10):

                month_value = date.today() - relativedelta(months=i)

                month_str = (str((month_value)).replace('-', '')[:-2])

                command1 = "hdfs dfs -rm -R -skipTrash /ranger/audit/hdfs/" + str(month_str) + "*"

                os.system(command1)

        print(" ")

        print(" ")

        print("--- --- --- --- --- --- --- --- --- --- --- ---")

        print("Deleting old folders from /ranger/audit/hiveServer2/")

        for i in range(3, 10):

                month_value = date.today() - relativedelta(months=i)

                month_str = (str((month_value)).replace('-', '')[:-2])

                command2 = "hdfs dfs -rm -R -skipTrash /ranger/audit/hiveServer2/" + str(month_str) + "*"

                os.system(command2)



if __name__ == '__main__':

        try:

		print(" ")

		print(" ")

		user = gt.getuser()

		hdfsUser = 'hdfs'

		if user == hdfsUser:

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

                        print(" ")

                        print(" ")

			time.sleep(0.5)

			print('Deleting old logs and files from various locations on HDFS')

                        print(" ")

                        time.sleep(1)

			clearAppLogs()

			time.sleep(1)

			#print(" ")

			time.sleep(0.5)

			#print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			remove_old_files_hive_llap_tar_gz()

			#print(" ")

                        time.sleep(0.5)

                        #print(" ")

                        time.sleep(0.5)

			#print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

                        print(" ")

                        PurgeRanger()

                        time.sleep(3)

                        print(" ")

                        print(" ")

                        print(" ")

                        print(" ")

                        #print("---- ---- ---- ----")

                        print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

                        print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

                        time.sleep(2)

			print('Logs and files from various locations on HDFS have been deleted')

			time.sleep(0.5)



			print(" ")

			print(" ")

			print(" ")

		else:

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			print('Please use this script with hdfs user')

			print("==== ==== ==== ==== ==== ==== ==== ==== ==== ====")

			print(" ")

			print(" ")



	except Exception as error:

		print(error)


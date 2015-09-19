#!/usr/bin/env python

# pxf_manual_failover.py
# This python script will adapt the PXF external tables to the new NameNode in case
# of High Availability manual failover. 
# The script receives as input the new namenode host and then goes over each external
# table entry in the catalog table pg_exttable and updates the LOCATION field - 
# replaces the old Namenode host with the new one. 

import sys
from gppylib.db import dbconn

def wrongUsage():
   'Print usage string and leave'
   print "usage: pxf_manual_failover <new_namenode_host> <database> [-h <hawq_master_host>] [-p <hawq_master_port>]"
   exit()

def getNewHost():
   'reads new NameNode from command line - exits if wrong input'
   if len(sys.argv) < 2:
      wrongUsage()
   return sys.argv[1]
   
def getDatabase():
   'reads database from command line - exits if wrong input'
   if len(sys.argv) < 3:
      wrongUsage()
   return sys.argv[2]
   
def getOptionalInput(flag, default):
   """generic function - retrieves optional parameters from the input
      If [flag <value>] is not on the command line, we use default 
      Explaining the parsing. This is how the command line string that
	  sys.argv returns, looks like:
	  ['./pxf_manual_failover.py', 'localhost', 'films', '-h', 'isenshackamac.corp.emc.com', '-p', '5432']
   """
   input = list(sys.argv)
   if input.count(flag) == 0:
      return default
   flag_idx = input.index(flag)
   if len(input) < flag_idx +1:
      wrongUsage()
   return input[flag_idx + 1]
   
def getMasterHost():
   'reads hawq_master_host from command line - optional'
   return getOptionalInput("-h", "localhost")
   
def getMasterPort():
   'reads hawq_master_port from command line - optional'
   return getOptionalInput("-p", 5432)
      
def isPxfTable(location):
   'decide if this is a PXF table by analyzing the LOCATION field for the table entry in pg_exttable'
   return cmp(location[1:7],"pxf://") == 0

def makeNewLocation(new_host, location):
   'replaces [host] substring in [location] with [new_host]'
   start = location.find("//")
   end = location.find(":", start)
   size = len(location)
   new_location = location[:start] + "//" + new_host + location[end:size]
   return new_location
   
def promptUser(new_host, database, hawq_master_host, hawq_master_port):
   'Give user a last chance to change his mind'
   print "Will replace the current Namenode hostname with [" + new_host + "] in database [" + database + "]"
   print "Hawq master is: [" + hawq_master_host + "] and Hawq port is: [" + str(hawq_master_port) + "]"
   reply = raw_input('Do you wish to continue: Yes[Y]/No[N] ?')
   reply = reply.lower()
   if not(cmp(reply, 'yes') == 0 or cmp(reply, 'y') == 0):
      print "User decided to cancel operation. Leaving..."
      exit() 

def connectToDb(hawq_master_host, hawq_master_port, database):
   'connect to database'
   url = dbconn.DbURL(hawq_master_host
                     ,port = hawq_master_port
				     ,dbname = database
                     )
   return dbconn.connect(dburl   = url)
   
def updateOneRecord(conn, new_host, row):
   'Updates the LOCATION field of one record'
   if not(isPxfTable(row[0])):
	   return
	   
   new_location = makeNewLocation(new_host, row[0])
   dbconn.execSQL(conn, "UPDATE pg_exttable SET location = '" + new_location + "' WHERE reloid = "
                        +  str(row[1]))
						
   print "Updated LOCATION for table ", row[2], "oid: ", row[1], \
		 "\n Old LOCATION: ", row[0], "\n New LOCATION: ", new_location
   
def updateNnHost(conn, new_host):
   'update the LOCATION field for each record in pg_exttable'
   dbconn.execSQL(conn, "set allow_system_table_mods = 'DML'")
   dbconn.execSQL(conn, "START TRANSACTION")
   cursor = dbconn.execSQL(conn, "SELECT location, reloid, relname FROM pg_exttable, pg_class WHERE reloid = relfilenode")
   for row in cursor:
      updateOneRecord(conn, new_host, row)
   conn.commit()
	  
def main():
   'The driver function of this module'
   new_host = getNewHost()
   database = getDatabase()
   hawq_master_host = getMasterHost()
   hawq_master_port = getMasterPort()
   promptUser(new_host, database, hawq_master_host, hawq_master_port)

   conn = connectToDb(hawq_master_host, hawq_master_port, database)
   updateNnHost(conn, new_host)

   conn.close()

if __name__ == "__main__":
   main()   	  
import csv
import datetime
import math

def distance(lon1, lat1, lon2, lat2):
  import numpy as np
  from math import sin, cos, sqrt, atan2, radians

  R = 6373.0

  lat1 = radians(lat1)
  lon1 = radians(lon1)
  lat2 = radians(lat2)
  lon2 = radians(lon2)

  dlon = lon2 - lon1
  dlat = lat2 - lat1

  a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
  c = 2 * atan2(sqrt(a), sqrt(1 - a))

  dist = R * c

  #print("Result:m", distance)
  return dist*1000	


#"TRIP_ID","CALL_TYPE","ORIGIN_	CALL","ORIGIN_STAND","TAXI_ID","TIMESTAMP","DAY_TYPE","MISSING_DATA","POLYLINE"
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e08')

with open("train.csv",'r') as csvfile:
   index = 0
   reader = csv.reader(csvfile, delimiter=',', quotechar='"')
   #skip reader
   next(reader, None)

   BATCH_STMT = "BEGIN BATCH "
   
   for row in reader:
     trip_id    = row[0]
     call_type  = row[1]
     origincall = row[2]
     originstand= row[3]
     taxi_id    = int(row[4])
     timestamp  = int(row[5])
     day_type   = row[6]
     missing    = row[7]
     list_coord = row[8]
        
     date  =  datetime.datetime.fromtimestamp(int(timestamp))
     year  = date.year
     month = date.month
     day   = date.day
     hour  = date.hour

         
    # ********************************* STATION TABLE ********************************************
     if originstand:
         INSERT_STMT = "INSERT INTO STATIONS (id) VALUES (%d)" % int(originstand) 
         prep_batch = session.execute(INSERT_STMT)
         print("STATION SUCCEDED! \n")


    # ********************************* TAXI TABLE ********************************************
     INSERT_STMT = "INSERT INTO TAXI (id) VALUES (%d)" % taxi_id
     prep_batch = session.execute(INSERT_STMT)
     print("TAXI SUCCEDED! \n")    

    # ********************************* TRIP TABLE ********************************************
     
     test = 0
   
     list_coord = list_coord[1:]
     list_coord = list_coord[:len(list_coord)-1]
     dist = 0

     if len(list_coord)>10:
       entry = []
       write = 0
       s = ""
       for a in list_coord:
         if a == ']':
          s+= str(a)
          entry.append(s)        
          write = 0
          s = "" 
       
         if write == 1:
           s+= str(a)

         if a == '[':
           s+= str(a) 
           write = 1

       #Get the origin
       tmp    = entry[0] 
       tmp    = tmp[1:]
       tmp    = tmp[:len(tmp)-1]
       ent1   = tmp.split(',')
       lon1   = float(ent1[0])
       lat1   = float(ent1[1])

       #Get destination
       tmp    = entry[len(entry)-1]
       tmp    = tmp[1:]
       tmp    = tmp[:len(tmp)-1]
       ent2   = tmp.split(',')
       lon2   = float(ent2[0])
       lat2   = float(ent2[1])

       #Compute the distance
       dist = distance(lon1, lat1, lon2, lat2)

     #Compute start_pave
     pavelon = math.floor(lon1*10)/10  
     pavelat = math.floor(lat1*10)/10 

     trimestre = 0
     if month>1 and month<4:
         trimestre = 1

     if month>3 and month<7:
         trimestre = 2

     if month>6 and month<10:
         trimestre = 3

     if month>9 and month<13:
         trimestre = 4

     if not origincall:
         origincall = 0
     else:
         origincal = int(origincall)

     if not originstand:
         originstand = 0
     else:
         originstand = int(originstand)

#tripid text, calltype text, origincall bigint, originstand bigint, taxiid bigint,timestamp timestamp,daytype text,
#missing text,gps text,year int,month int,trimestre int,day int,hour int, startlong float, startlat float,
#endlong float, endlat float, distance float, pavelong float, pavelat float,

     INSERT_STMT = "INSERT INTO TRIP1 (tripid, calltype, origincall, originstand, taxiid, timestamp, daytype, missing, year, month, trimestre, day, hour, startlong, startlat, endlong, endlat, distance, pavelong, pavelat) VALUES ('%s', '%c', %d , %d, %d, %d, '%c', '%s', %d, %d, %d, %d, %d, %f, %f, %f, %f, %f, %f, %f)" % (trip_id, call_type, int(origincall), int(originstand), taxi_id, timestamp, day_type, missing, int(year), int(month), trimestre, int(day), int(hour), lon1, lat1, lon2, lat2, float(dist), pavelon, pavelat)

     prep_batch = session.execute(INSERT_STMT)
     print("TRIP1 SUCCEDED! \n")


       
     INSERT_STMT = "INSERT INTO TRIP2 (tripid, calltype, origincall, originstand, taxiid, timestamp, daytype, missing, year, month, trimestre, day, hour, startlong, startlat, endlong, endlat, distance, pavelong, pavelat) VALUES ('%s', '%c', %d , %d, %d, %d, '%c', '%s', %d, %d, %d, %d, %d, %f, %f, %f, %f, %f, %f, %f)" % (trip_id, call_type, int(origincall), int(originstand), taxi_id, timestamp, day_type, missing, int(year), int(month), trimestre, int(day), int(hour), lon1, lat1, lon2, lat2, float(dist), pavelon, pavelat)

     prep_batch = session.execute(INSERT_STMT)
     print("TRIP2 SUCCEDED! \n")



     INSERT_STMT = "INSERT INTO TRIP3 (tripid, calltype, origincall, originstand, taxiid, timestamp, daytype, missing, year, month, trimestre, day, hour, startlong, startlat, endlong, endlat, distance, pavelong, pavelat) VALUES ('%s', '%c', %d , %d, %d, %d, '%c', '%s', %d, %d, %d, %d, %d, %f, %f, %f, %f, %f, %f, %f)" % (trip_id, call_type, int(origincall), int(originstand), taxi_id, timestamp, day_type, missing, int(year), int(month), trimestre, int(day), int(hour), lon1, lat1, lon2, lat2, float(dist), pavelon, pavelat)

     prep_batch = session.execute(INSERT_STMT)
     print("TRIP3 SUCCEDED! \n")


     INSERT_STMT = "INSERT INTO TRIP4 (tripid, calltype, origincall, originstand, taxiid, timestamp, daytype, missing, year, month, trimestre, day, hour, startlong, startlat, endlong, endlat, distance, pavelong, pavelat) VALUES ('%s', '%c', %d , %d, %d, %d, '%c', '%s', %d, %d, %d, %d, %d, %f, %f, %f, %f, %f, %f, %f)" % (trip_id, call_type, int(origincall), int(originstand), taxi_id, timestamp, day_type, missing, int(year), int(month), trimestre, int(day), int(hour), lon1, lat1, lon2, lat2, float(dist), pavelon, pavelat)

     prep_batch = session.execute(INSERT_STMT)
     print("TRIP4 SUCCEDED! \n")


     #BATCH_STMT += INSERT_STMT
     #index += 1

     #Insert each 100 entries
     #if index==10:
         #BATCH_STMT += " APPLY BATCH;"
         #prep_batch = session.execute(BATCH_STMT)
	 #print("INSERT SUCCEDED! \n")
         #index = 0
         #BATCH_STMT = "BEGIN BATCH "
       

#Insert the remainin      
#if index>0:
        #BATCH_STMT += "APPLY BATCH;"
        #prep_batch = session.prepare(BATCH_STMT)
        #session.execute(reqA)
        #index = 0
        #BATCH_STMT = "BEGIN BATCH "






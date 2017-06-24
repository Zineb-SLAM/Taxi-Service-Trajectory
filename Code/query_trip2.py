import matplotlib
matplotlib.use('Agg') 

import matplotlib.pyplot as plt

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e08')



#*************tous les trajets qui partent d un pave donne qui partent entre le 10 avril 2004 et le 10 juin 2004************
taxi_query_trip2 = session.execute("SELECT month, daytype, pavelong, pavelat, tripid FROM TRIP2 WHERE year=2013 AND pavelong=-8.1 AND pavelat=41.1")

#GROUP BY MONTH
month     = [1,2,3,4,5,6,7,8,9,10,11,12]
nb_by_month = [0,0,0,0,0,0,0,0,0,0,0,0]

for user_row in taxi_query_trip2:
    nb_by_month[user_row.month-1] += 1

#GROUP BY DAY-TYPE
taxi_query_trip2 = session.execute("SELECT month, daytype, tripid FROM TRIP2 WHERE year=2013 AND pavelong=-8.1 AND pavelat=41.1")
nb_by_daytype   = {'A':0, 'B':0, 'C':0}

for user_row in taxi_query_trip2:
    nb_by_daytype[user_row.daytype] += 1



#******************************************PLOT*********************************************
plt.plot(month, nb, marker='o', color='b')
plt.xlabel('Month')
plt.ylabel('Trip count')
plt.title('Number of trips departing from pave')
plt.legend(['trips count'], loc='upper left')
plt.savefig('Figures/trips_departing_pave.png')




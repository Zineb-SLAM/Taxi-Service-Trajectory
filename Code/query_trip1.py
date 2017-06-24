import matplotlib
matplotlib.use('Agg') 

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e08')

import matplotlib.pyplot as plt

import kmeans

#********************************Evolution du  nombre de trajet par heure********************************************
#Test Without Group by
taxi_query_trip1 = session.execute("SELECT day, hour, tripid FROM TRIP1 WHERE year=2013 AND month=9 AND day=9")
hour = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]
tripnb = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
for user_row in taxi_query_trip1:
    tripnb[user_row.hour] += 1
        

#Test Without Group by
#taxi_query_trip1 = session.execute("SELECT day, hour, COUNT(tripid) as nb FROM TRIP1 WHERE year=2013 AND month=9 AND day=9 GROUP by hour")
#hour = []
#tripnb = []
#for user_row in taxi_query_trip1:
#    hour.append(int(user_row.hour))
#    tripnb.append(int(user_row.nb))

plt.plot(hour, tripnb, marker='o', color='b')
plt.xlabel('Hours')
plt.ylabel('Trip count')
plt.title('Trip number over hour')
plt.legend(['trips count'], loc='upper left')
plt.savefig('Figures/trips_by_day.png')

#**********************************************Distance parcourues par heure********************************************* 
#taxi_query2_trip1 = session.execute("SELECT day, hour, SUM(distance) as distance FROM TRIP1 WHERE year=2013 AND month=9 AND day=9 GROUP by hour")

taxi_query2_trip1 = session.execute("SELECT day, hour, distance FROM TRIP1 WHERE year=2013 AND month=9 AND day=9")
distance = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
for user_row in taxi_query2_trip1:
    distance[user_row.hour] += user_row.distance

plt.clf()
plt.plot(hour, distance, marker='o', color='r')
plt.xlabel('Hours')
plt.ylabel('Distance')
plt.title('Distance over hour')
plt.legend(['trip distance'], loc='upper left')
plt.savefig('Figures/distance_by_day.png')


#**********************************************Kmeans + PLOT********************************************* 

#diffence des concentration /direction de trajets entre 9h du mation et 8h du soir
query = ("SELECT startlong, startlat, endlong, endlat, distance FROM TRIP1 WHERE year=2013 AND month=9 AND day=9 AND hour=10")
clusters = cluster(query)

lats1 = []
longs1 = []
len1  = len(clusters[0])
for i in range(len1):
  lats1.append(clusters[0][i][0])
  longs1.append(clusters[0][i][1])

lats2 = []
longs2 = []
len2  = len(clusters[1])
for i in range(len2):
  lats2.append(clusters[1][i][0])
  longs2.append(clusters[1][i][1])

lats3 = []
longs3 = []
len3  = len(clusters[2])
for i in range(len3):
  lats3.append(clusters[2][i][0])
  longs3.append(clusters[2][i][1])


plt.clf()
plt.scatter(lats1, longs1)
plt.scatter(lats2, longs2)
plt.scatter(lats3, longs3)
plt.savefig('Figures/kmeans_10AM.png')




query = ("SELECT startlong, startlat, endlong, endlat, distance FROM TRIP1 WHERE year=2013 AND month=9 AND day=9 AND hour=20")
clusters = cluster(query)

lats1 = []
longs1 = []
len1  = len(clusters[0])
for i in range(len1):
  lats1.append(clusters[0][i][0])
  longs1.append(clusters[0][i][1])

lats2 = []
longs2 = []
len2  = len(clusters[1])
for i in range(len2):
  lats2.append(clusters[1][i][0])
  longs2.append(clusters[1][i][1])

lats3 = []
longs3 = []
len3  = len(clusters[2])
for i in range(len3):
  lats3.append(clusters[2][i][0])
  longs3.append(clusters[2][i][1])


plt.clf()
plt.scatter(lats1, longs1)
plt.scatter(lats2, longs2)
plt.scatter(lats3, longs3)
plt.savefig('Figures/kmeans_8PM.png')





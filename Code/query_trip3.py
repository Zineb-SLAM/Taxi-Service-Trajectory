import matplotlib
matplotlib.use('Agg') 

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e08')

import matplotlib.pyplot as plt

#Les stations
query_taxi = session.execute("SELECT id FROM TAXI;")

taxi = []
nb = []

 #La concentration des station, par annee et par periode de l'annee
for user_row in query_taxi: 
     query= "SELECT COUNT(tripid) as nb FROM TRIP3 WHERE year=2013 AND month=9 AND taxiid=" + str(user_row.id) + ";"  
     query_trip3 = session.execute(query)
     taxi.append(int(user_row.id))

     for user_row2 in query_trip3:
         nb.append(int(user_row2.nb))



#************************************PLOT HISTOGRAMS******************************#
plt.clf()
plt.bar(taxi, nb)
plt.title("TRIPS BY TAXI")
plt.savefig('Figures/histogram_trip_by_taxi.png')


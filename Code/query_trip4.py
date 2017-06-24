import matplotlib
matplotlib.use('Agg') 

from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect('e08')


#Les stations
query_station = session.execute("SELECT id FROM stations;")

stations = []
nb = []

 #La concentration des station, par annee et par periode de l'annee
for user_row in query_station: 
     query= "SELECT COUNT(tripid) as nb FROM TRIP4 WHERE year=2013 AND month=9 AND originstand=" + str(user_row.id) + ";"  
     query_trip3 = session.execute(query)
     stations.append(user_row.id)

     for user_row2 in query_trip3:
         nb.append(int(user_row2.nb))



#*********************************************PLOT HISTOGRAMS*************************************#
plt.clf()
plt.bar(stations,nb)  # plt.hist passes it's arguments to np.histogram
plt.title("TRIPS FROM STATIONS")
plt.savefig('Figures/histogram_trip_from_station.png')


#Computes the distance between 2 points
def distance(lon1, lat1, lon2, lat2):
   dist = (lon2-lon1)**(2) + (lat2-lat1)**(2)
   return (dist)

#Computes the distance between a point and a centroid = start_distance + end_distance
def double_distance (centroid, entry):
    d1 = distance(centroid['startlong'], centroid['startlat'], entry.startlong, entry.startlat)
    d2 = distance(centroid['endlong'], centroid['endlat'], entry.endlong, entry.endlat)
    d = d1 + d2
    return (d**(1/2))
    

def kmeans(query):
    from cassandra.cluster import Cluster
    from random import randint

    cluster = Cluster()
    session = cluster.connect('e08')

    STOP = 0
    ittr_conv = 0
    nb_clusters = 3
    N = 0
    query_trip1 = session.execute(query)
   
    #************************COUNT NUMBER OF ELEMENTS/ROWS RETURNED******************************#
    for row in query_trip1:
      N = N+1
    
    #************************GET RANDOM INDEXES FOR CENTROIDS******************************#
    centroids_indx = [randint(0,N) for p in range(0,nb_clusters)]
    centroids_indx = sorted(centroids_indx, key=int)
    
    counter = 0
    index  = 0
    init_centroids = []
   
    query_trip1 = session.execute(query)
    for row in query_trip1:
        if counter == centroids_indx[index]:
            centroid = [row.startlong, row.startlat, row.endlong, row.endlat]
            init_centroids.append(centroid)

            index = index +1 

        if index == nb_clusters:
           break;        

        counter = counter + 1
        
    #************************INITIALIZE CENTROIDS *****************************#
    oldcentroids = [{'startlong':0,'startlat':0,'endlong':0,'endlat':0}, {'startlong':0,'startlat':0,'endlong':0,'endlat':0}, {'startlong':0,'startlat':0,'endlong':0,'endlat':0}]

    newcentroids = [{'startlong':init_centroids[0][0], 'startlat':init_centroids[0][1], 'endlong':init_centroids[0][2], 'endlat':init_centroids[0][3]}, {'startlong':init_centroids[1][0], 'startlat':init_centroids[1][1], 'endlong':init_centroids[1][2], 'endlat':init_centroids[1][3]}, {'startlong':init_centroids[2][0], 'startlat':init_centroids[2][1], 'endlong':init_centroids[2][2], 'endlat':init_centroids[2][3]}]


    #************************TIME TO STARTI ITTERATING WHILE NOT CONVERGING*****************************#
    while(STOP == 0):
       query_trip1 = session.execute(query)
       coord_sums = [{'startlong':0,'startlat':0,'endlong':0,'endlat':0 ,'nb':0},{'startlong':0,'startlat':0,'endlong':0,'endlat':0,'nb':0}, {'startlong':0,'startlat':0,'endlong':0,'endlat':0,'nb':0}]

       for row in query_trip1:
           distance1 = double_distance(newcentroids[0], row) 
           distance2 = double_distance(newcentroids[1], row)
           distance3 = double_distance(newcentroids[2], row)

           distances  = [distance1, distance2, distance3]

           min_indx = distances.index(min(distances))

           coord_sums[min_indx]['startlong'] += row.startlong
           coord_sums[min_indx]['startlat']  += row.startlat
           coord_sums[min_indx]['endlong']   += row.endlong
           coord_sums[min_indx]['endlat']    += row.endlat
           coord_sums[min_indx]['nb']        += 1


       #************************NEW CENTROIDS******************************#
       oldcentroids = newcentroids
       for i in range(nb_clusters):
            newcentroids[i]['startlong'] =  coord_sums[i]['startlong']/coord_sums[i]['nb']
            newcentroids[i]['startlat']  =  coord_sums[i]['startlat']/coord_sums[i]['nb']
            newcentroids[i]['endlong']   =  coord_sums[i]['endlong']/coord_sums[i]['nb']
            newcentroids[i]['endlat']    =  coord_sums[i]['endlat']/coord_sums[i]['nb']

        #************************SHOUDL WE STOP******************************#
       ittr_conv = ittr_conv + 1
       if oldcentroids == newcentroids:
          STOP = 1
      
    return(newcentroids)  


def cluster(query):
    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect('e08')

    centroids = kmeans(query)
    query_trip1 = session.execute(query)
    cluster1 = []
    cluster2 = []
    cluster3 = []

    for row in query_trip1:
        distance1 = double_distance(centroids[0], row) 
        distance2 = double_distance(centroids[1], row)
        distance3 = double_distance(centroids[2], row)
        distances  = [distance1, distance2, distance3]
        min_indx = distances.index(min(distances))

        if (min_indx == 0):
              cluster1.append(row)
        if (min_indx == 1):
              cluster2.append(row)
        if (min_indx == 2):
              cluster3.append(row)

    clusters = [cluster1, cluster2, cluster3]
    return (clusters)


        
    
    
   
    

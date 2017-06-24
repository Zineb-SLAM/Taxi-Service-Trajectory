CREATE TABLE TRIP1 ( 
tripid text, 
calltype text, 
origincall bigint, 
originstand bigint,
taxiid bigint,
timestamp timestamp,
daytype text,
missing text,
year int,
month int,
trimestre int,
day int,
hour int,
startlong float,
startlat float,
endlong float,
endlat float,
distance float,
pavelong float,
pavelat float,
PRIMARY KEY ((year,month,day), hour,tripid));


CREATE TABLE TRIP2 ( 
tripid text, 
calltype text, 
origincall bigint, 
originstand bigint,
taxiid bigint,
timestamp timestamp,
daytype text,
missing text,
year int,
month int,
trimestre int,
day int,
hour int,
startlong float,
startlat float,
endlong float,
endlat float,
distance float,
pavelong float,
pavelat float,
PRIMARY KEY ((pavelong, pavelat, year), month, day, hour, tripid));


CREATE TABLE TAXI(
id bigint,
PRIMARY KEY(id));


CREATE TABLE TRIP3 (  
tripid text, 
calltype text, 
origincall bigint, 
originstand bigint,
taxiid bigint,
timestamp timestamp,
daytype text,
missing text,
year int,
month int,
trimestre int,
day int,
hour int,
startlong float,
startlat float,
endlong float,
endlat float,
distance float,
pavelong float,
pavelat float,
PRIMARY KEY ((taxiid, year, month), day, hour, tripid));


CREATE TABLE STATIONS(
id bigint,
PRIMARY KEY(id));

CREATE TABLE TRIP4 ( 
tripid text, 
calltype text, 
origincall bigint, 
originstand bigint,
taxiid bigint,
timestamp timestamp,
daytype text,
missing text,
year int,
month int,
trimestre int,
day int,
hour int,
startlong float,
startlat float,
endlong float,
endlat float,
distance float,
pavelong float,
pavelat float,
PRIMARY KEY ((originstand, year, month), day, hour, tripid));









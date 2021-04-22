import requests
import json
from pprint import pprint
import psycopg2
import pubg_stats
from PUBGkeys import *


#search for matches, check against db to create list of new matches
match_ids = pubg_stats.get_match_ids()
db_match_ids = pubg_stats.query_db_for_match_list()
new_match_ids = []
for i in match_ids:
	if i not in db_match_ids:
		new_match_ids.append(i)


## write new matches to db if new matches
if len(new_match_ids) != 0:
	
	try:
	   connection = psycopg2.connect(user=db_u,
	                                  password=db_pw,
	                                  host="127.0.0.1",
	                                  port="5432",
	                                  database="PUBG")	
	   cursor = connection.cursor()
	   
	   for match in new_match_ids:
	   		match_stats = pubg_stats.get_stats_for_match(match)
	   		for i in match_stats:
	   			if 'matchHeader' in i:
	   				(query, values) = pubg_stats.insert_query_match_table(i)
	   				cursor.execute(query,values)
	   			elif i['participant'] == 'Rovey Wade':
	   				(query, values) = pubg_stats.insert_query_player_table('roveywade',i)
	   				cursor.execute(query,values)
	   			elif i['participant'] == 'Likethfruit':
	   				(query, values) = pubg_stats.insert_query_player_table('likethfruit',i)
	   				cursor.execute(query,values)
	   			elif i['participant'] == 'bbccde':
	   				(query, values) = pubg_stats.insert_query_player_table('bbccde',i)
	   				cursor.execute(query,values)
	   			elif i['participant'] == 'Rootsmasher':
	   				(query, values) = pubg_stats.insert_query_player_table('rootsmasher',i)
	   				cursor.execute(query,values)

	   connection.commit()
	   
	   print ("Records inserted successfully into matches table")

	except (Exception, psycopg2.Error) as error :
	    if(connection):
	        print("Failed to insert record into matches table", error)

	finally:
	    #closing database connection.
	    if(connection):
	        cursor.close()
	        connection.close()
	        print("PostgreSQL connection is closed")












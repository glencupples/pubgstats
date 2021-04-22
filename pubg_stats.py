import requests
import json
from pprint import pprint
from psycopg2.extensions import AsIs
import psycopg2
import pubg_stats
from PUBGkeys import *




header = {
  "Authorization": API_key,
  "Accept": "application/vnd.api+json"
}

base_url = 'https://api.pubg.com/shards/xbox/'

player_names = ['Rovey Wade','bbccde','Likethfruit','Rootsmasher']



def get_match_ids():
	#get match ids as a list
	# player_names = ['Rovey Wade','bbccde','Likethfruit','Rootsmasher']
	match_ids = []
	for name in player_names:
		player_url = "{}players?filter[playerNames]={}".format(base_url,name)
		player_response = requests.get(player_url, headers=header)
		player_matches = player_response.json()
		player_matches = player_matches['data'][0]['relationships']['matches']['data']
		match_ids.extend([di['id'] for di in player_matches])
	match_ids = list(set(match_ids)) #create list of set to remove duplicates
	return match_ids

def get_stats_for_match(match_id):
	#get match stats based on match id
	matches_url = "{}matches/{}".format(base_url, match_id)
	response = requests.get(matches_url, headers=header)
	results = response.json()
	match_header = {'matchHeader':results['data']['attributes']}
	results = results['included']
	player_stats = [match_header]
	participant_names = []
	particpant_IDs = []

	#get stats for players in list
	for i in results:		
		if (i.get('type') == 'participant' and i.get('attributes').get('stats').get('name') in player_names):
			stat_block = {} #stat block with both participant name and stats to add another level on the player_stats dict
			stat_block['participant'] = i.get('attributes').get('stats')['name'] 
			stat_block['stats'] = i.get('attributes').get('stats')
			stat_block['stats']['matchID'] = match_id
			player_stats.append(stat_block)
			particpant_IDs.append(i.get('id'))
			participant_names.append(i.get('attributes').get('stats')['name'])


	#get team rank
	roster_stats = []
	for i in results:
		if i.get('type') == 'roster':
			iter_roster = i.get('relationships').get('participants').get('data')
			for r in iter_roster:
				if r.get('id') in particpant_IDs:
					roster_stats.append(i.get('attributes').get('stats'))
					break

	#get participant count
	participant_count = 0
	for i in results:
		if(i.get('type')) == 'participant':
			participant_count +=1
							
	#add team rank, match ID, participant count and participants list in match header
	player_stats[0]['matchHeader']['rosterRank'] = roster_stats[0]['rank']
	player_stats[0]['matchHeader']['matchID'] = match_id
	player_stats[0]['matchHeader']['participantCount'] = participant_count
	player_stats[0]['matchHeader']['participants'] = participant_names
	return player_stats

def query_db_for_match_list():				##returns list of db match ids to compare against
	try:
	   connection = psycopg2.connect(user=db_u,
	                                  password=db_pw,
	                                  host="127.0.0.1",
	                                  port="5432",
	                                  database="PUBG")
	   cursor = connection.cursor()
	   postgreSQL_select_Query = "select match_id from matches"
	   cursor.execute(postgreSQL_select_Query)
	   db_matches = [r[0] for r in cursor.fetchall()]
	   return db_matches
	   
	  
	       

	except (Exception, psycopg2.Error) as error :
	    print ("Error while fetching data from PostgreSQL", error)

	finally:
	    #closing database connection.
	    if(connection):
	        cursor.close()
	        connection.close()
	        print("PostgreSQL connection is closed")

def insert_query_match_table(stat_block):
	match_attributes = stat_block['matchHeader']
	query = """INSERT INTO matches(created_at, duration, match_type, game_mode, map_name, match_id, roster_rank, player_count, participants)
   									VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
	values = (match_attributes['createdAt'], match_attributes['duration'],match_attributes['matchType'],match_attributes['gameMode'], 
			match_attributes['mapName'], match_attributes['matchID'], match_attributes['rosterRank'], match_attributes['participantCount'],
			match_attributes['participants'])
	return (query, values)

def insert_query_player_table(player,stat_block):
	player_stats = stat_block.get('stats')

	query = """INSERT INTO {}(dbnos, assists, boosts, damage_dealt, death_type, headshot_kills, heals, kill_place, kill_streaks,
								kills, longest_kill, match_id, name, revives, ride_distance, road_kills, swim_distance, team_kills, time_survived, vehicles_destroyed,
								walk_distance, weapons_acquired, win_place)
   									VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""".format(player)
	values = (player_stats['DBNOs'], player_stats['assists'],player_stats['boosts'],player_stats['damageDealt'], 
			player_stats['deathType'], player_stats['headshotKills'], player_stats['heals'], player_stats['killPlace'],
			player_stats['killStreaks'], player_stats['kills'], player_stats['longestKill'], player_stats['matchID'], player_stats['name'], player_stats['revives'],
			player_stats['rideDistance'], player_stats['roadKills'], player_stats['swimDistance'], player_stats['teamKills'],
			player_stats['timeSurvived'], player_stats['vehicleDestroys'], player_stats['walkDistance'], player_stats['weaponsAcquired'],
			player_stats['winPlace'])
	return (query, values)






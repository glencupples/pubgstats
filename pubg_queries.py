import psycopg2
from PUBGkeys import *

#main query
def query(query_text):
	try:
	   connection = psycopg2.connect(user=db_u,
	                                  password=db_pw,
	                                  host="127.0.0.1",
	                                  port="5432",
	                                  database="PUBG")
	   cursor = connection.cursor()
	   postgreSQL_select_Query = query_text
	   cursor.execute(postgreSQL_select_Query)
	   return cursor.fetchall()

	except (Exception, psycopg2.Error) as error :
	    print ("Error while fetching data from PostgreSQL", error)

	finally:
	    #closing database connection.
	    if(connection):
	        cursor.close()
	        connection.close()

### sort and remove empties (if someone doesn't have a chicken dinner, e.g.)
def player_sort(stat_list):
	stat_list = [i for i in stat_list if i[0] != None]			
	stat_list.sort(reverse = True, key = lambda x: x[1])  
	return stat_list


### matches played
def matches_played():
	return player_sort(query('''SELECT MIN(roveywade.name), COUNT(roveywade.match_id)
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), COUNT(bbccde.match_id)
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'


					UNION
					SELECT MIN(rootsmasher.name), COUNT(rootsmasher.match_id)
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'	

					UNION
					SELECT MIN(likethfruit.name), COUNT(likethfruit.match_id)
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))

### chicken dinner count for week
def chicken_dinner_count():
	return player_sort(query('''SELECT MIN(roveywade.name), COUNT(roveywade.match_id)
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE matches.roster_rank = 1 AND TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), COUNT(bbccde.match_id)
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE matches.roster_rank = 1 AND TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), COUNT(rootsmasher.match_id)
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE matches.roster_rank = 1 AND TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(likethfruit.name), COUNT(likethfruit.match_id)
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE matches.roster_rank = 1 AND TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))



### longest kill of the week
def longest_kill():
	return player_sort(query('''(SELECT roveywade.name, roveywade.longest_kill
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					ORDER BY longest_kill DESC 
					LIMIT 1)
					UNION

					(SELECT bbccde.name, bbccde.longest_kill
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					ORDER BY longest_kill DESC
					LIMIT 1)
					UNION


					(SELECT rootsmasher.name, rootsmasher.longest_kill
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					ORDER BY longest_kill DESC
					LIMIT 1)
					UNION

					(SELECT likethfruit.name, likethfruit.longest_kill
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					ORDER BY longest_kill DESC
					LIMIT 1);'''))
	   
### higest team rank of the week
def highest_team_rank():
	return player_sort(query('''(SELECT roveywade.name, matches.roster_rank
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					ORDER BY matches.roster_rank
					LIMIT 1)
					UNION

					(SELECT bbccde.name, matches.roster_rank
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					ORDER BY matches.roster_rank
					LIMIT 1)
					UNION


					(SELECT rootsmasher.name, matches.roster_rank
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					ORDER BY matches.roster_rank
					LIMIT 1)
					UNION

					(SELECT likethfruit.name, matches.roster_rank
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					ORDER BY matches.roster_rank
					LIMIT 1);'''))


### k:d of the week
def kill_to_death():
	return player_sort(query('''SELECT MIN(roveywade.name) as name, 
					TRUNC((CAST(SUM(roveywade.kills) AS DECIMAL)/SUM(CASE WHEN roveywade.death_type = 'alive' THEN 0 ELSE 1 END)),2) as "k:d"
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					UNION

					SELECT MIN(bbccde.name) as name, 
							TRUNC((CAST(SUM(bbccde.kills) AS DECIMAL)/SUM(CASE WHEN bbccde.death_type = 'alive' THEN 0 ELSE 1 END)),2) as "k:d"
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					UNION

					SELECT MIN(rootsmasher.name) as name, 
							TRUNC((CAST(SUM(rootsmasher.kills) AS DECIMAL)/SUM(CASE WHEN rootsmasher.death_type = 'alive' THEN 0 ELSE 1 END)),2) as "k:d"
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'
					UNION

					SELECT MIN(likethfruit.name) as name, 
							TRUNC((CAST(SUM(likethfruit.kills) AS DECIMAL)/SUM(CASE WHEN likethfruit.death_type = 'alive' THEN 0 ELSE 1 END)),2) as "k:d"
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY';'''))

### most kills this week
def kills():
	return player_sort(query('''SELECT MIN(roveywade.name), SUM(roveywade.kills)
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), SUM(bbccde.kills)
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), SUM(rootsmasher.kills)
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(likethfruit.name), SUM(likethfruit.kills)
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))

### most damage this week
def damage():
	return player_sort(query('''SELECT MIN(roveywade.name), TRUNC(SUM(roveywade.damage_dealt))
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), TRUNC(SUM(bbccde.damage_dealt))
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), TRUNC(SUM(rootsmasher.damage_dealt))
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(likethfruit.name), TRUNC(SUM(likethfruit.damage_dealt))
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))


### most headshots this week
def headshot_kills():
	return player_sort(query('''SELECT MIN(roveywade.name), SUM(roveywade.headshot_kills)
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), SUM(bbccde.headshot_kills)
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), SUM(rootsmasher.headshot_kills)
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(likethfruit.name), SUM(likethfruit.headshot_kills)
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))


### most assists this week
def assists():
	return player_sort(query('''SELECT MIN(roveywade.name), SUM(roveywade.assists)
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), SUM(bbccde.assists)
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), SUM(rootsmasher.assists)
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'


					UNION
					SELECT MIN(likethfruit.name), SUM(likethfruit.assists)
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))


### most revives this week
def revives():
	return player_sort(query('''SELECT MIN(roveywade.name), SUM(roveywade.revives)
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), SUM(bbccde.revives)
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), SUM(rootsmasher.revives)
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(likethfruit.name), SUM(likethfruit.revives)
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))

def dbnos():
	return player_sort(query('''SELECT MIN(roveywade.name), SUM(roveywade.dbnos)
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), SUM(bbccde.dbnos)
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), SUM(rootsmasher.dbnos)
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(likethfruit.name), SUM(likethfruit.dbnos)
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))	

### most distance walked this week
def distance_walked():
	return player_sort(query('''SELECT MIN(roveywade.name), TRUNC(SUM(roveywade.walk_distance))
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), TRUNC(SUM(bbccde.walk_distance))
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), TRUNC(SUM(rootsmasher.walk_distance))
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(likethfruit.name), TRUNC(SUM(likethfruit.walk_distance))
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))


### average time survived (minutes:seconds)
def avg_time_survived():
	return player_sort(query('''SELECT MIN(roveywade.name), TO_CHAR((AVG(roveywade.time_survived)|| 'second')::interval, 'MI:SS')
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(bbccde.name), TO_CHAR((AVG(bbccde.time_survived)|| 'second')::interval, 'MI:SS')
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(rootsmasher.name), TO_CHAR((AVG(rootsmasher.time_survived)|| 'second')::interval, 'MI:SS')
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT MIN(likethfruit.name), TO_CHAR((AVG(likethfruit.time_survived)|| 'second')::interval, 'MI:SS')
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))

### average percentage of bots in round
def avg_bots_in_match():
	return player_sort(query('''SELECT STRING_AGG(DISTINCT roveywade.name, ','), 
								TO_CHAR(TRUNC(AVG(CASE WHEN matches.map_name = 'Summerland_Main' 
								THEN ((64-CAST(matches.player_count AS DECIMAL))/64)*100
								ELSE ((100-CAST(matches.player_count AS DECIMAL))/100)*100
								END),2),'99D99%') as bot_pct
					FROM matches
					JOIN roveywade ON matches.match_id = roveywade.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT STRING_AGG(DISTINCT bbccde.name, ','), 
								TO_CHAR(TRUNC(AVG(CASE WHEN matches.map_name = 'Summerland_Main' 
								THEN ((64-CAST(matches.player_count AS DECIMAL))/64)*100
								ELSE ((100-CAST(matches.player_count AS DECIMAL))/100)*100
								END),2),'99D99%') as bot_pct
					FROM matches
					JOIN bbccde ON matches.match_id = bbccde.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT STRING_AGG(DISTINCT likethfruit.name, ','), 
								TO_CHAR(TRUNC(AVG(CASE WHEN matches.map_name = 'Summerland_Main' 
								THEN ((64-CAST(matches.player_count AS DECIMAL))/64)*100
								ELSE ((100-CAST(matches.player_count AS DECIMAL))/100)*100
								END),2),'99D99%') as bot_pct
					FROM matches
					JOIN likethfruit ON matches.match_id = likethfruit.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY'

					UNION
					SELECT STRING_AGG(DISTINCT rootsmasher.name, ','), 
								TO_CHAR(TRUNC(AVG(CASE WHEN matches.map_name = 'Summerland_Main' 
								THEN ((64-CAST(matches.player_count AS DECIMAL))/64)*100
								ELSE ((100-CAST(matches.player_count AS DECIMAL))/100)*100
								END),2),'99D99%') as bot_pct
					FROM matches
					JOIN rootsmasher ON matches.match_id = rootsmasher.match_id
					WHERE TO_DATE(created_at,'YYYY:MM:DD') > NOW() - INTERVAL '8 DAY' '''))	

def display(category_name, query_list, per_game):
	if per_game == True: ##per game included
		match_count = dict(matches_played())
		display = category_name + '\n'
		for x in query_list:
			display += (x[0] + ": " + str(x[1]) +  " (" + str(round(x[1]/match_count[x[0]],2)) + ")" + '\n')
		return display
	else: ## just total count
		display = category_name + '\n'
		for x in query_list:
			display += (x[0] + ": " + str(x[1])+ '\n')
		return display



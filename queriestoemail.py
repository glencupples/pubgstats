import yagmail
import pubg_queries
from EmailPassword import password




#email it out
yag = yagmail.SMTP("glen.cupples.dev@gmail.com",password)
contents = [
	pubg_queries.display('Matches Played',pubg_queries.matches_played(), False), '\n',
	pubg_queries.display('Chicken Dinners',pubg_queries.chicken_dinner_count(), False), '\n',
	pubg_queries.display('Highest Team Rank',pubg_queries.highest_team_rank(), False), '\n',	
	pubg_queries.display('Total Kills',pubg_queries.kills(), True), '\n',
	pubg_queries.display('Damage',pubg_queries.damage(), True), '\n',
	pubg_queries.display('KDR',pubg_queries.kill_to_death(), False), '\n',
	pubg_queries.display('Longest Kill',pubg_queries.longest_kill(), False), '\n',
	pubg_queries.display('Headshot Kills',pubg_queries.headshot_kills(), True), '\n',
	pubg_queries.display('Assists',pubg_queries.assists(), True), '\n',
	pubg_queries.display('Revives',pubg_queries.revives(), True), '\n',
	pubg_queries.display('Down But Not Outs',pubg_queries.dbnos(), True), '\n',
	pubg_queries.display('Average Time Survived',pubg_queries.avg_time_survived(), False), '\n',
	pubg_queries.display('Average Percentage of Bots in Match',pubg_queries.avg_bots_in_match(), False), '\n',
	pubg_queries.display('Total Distance Walked',pubg_queries.distance_walked(), True)
	]
yag.send(['glen.cupples@gmail.com','mattmcgahen@gmail.com','dradamniemuth@gmail.com','per.k.ohrstrom@gmail.com'], 'Weekly PUBG Stats', contents)

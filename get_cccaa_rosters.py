#from selenium import webdriver
import time
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
#from unidecode import unidecode
import requests

def get_CCCAA_Rosters():
	teams_df = pd.read_csv('cccaa_schools.csv')
	school_name = teams_df['school_name'].tolist()
	school_njcaa_season = teams_df['cccaa_season'].tolist()
	#school_njcaa_division = teams_df['division'].tolist()
	school_team_id = teams_df['team_id'].tolist()
	school_season = teams_df['season'].tolist()
	# driver = webdriver.Chrome(
	# 	executable_path=webdriverPath)
	
	roster_df = pd.DataFrame()
	for i in tqdm(range(0,len(school_name))):
		roster_df = pd.DataFrame()
		## Declarations to prevent "local variable referenced before assignment" errors
		player_num = None
		player_name = None
		player_position = None
		player_year = None
		player_url = None
		player_id = None

		## This data is in teams_df
		team_name = school_name[i]
		team_cccaa_season = school_njcaa_season[i]
		team_id = school_team_id[i]
		team_season = school_season[i]
		print(team_cccaa_season,team_name)
		url = f"https://www.cccaasports.org/sports/bsb/{team_cccaa_season}/teams/{team_id}"
		print(url)
		headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
		response = requests.get(url,headers=headers)
		soup = BeautifulSoup(response.text, features='lxml')
		try:
			table = soup.find_all('table')[4]

			count = 0

			#print(table)
			for k in table.find_all('tr'):
				row = k.find_all('td')
				if len(row) == 0:
					pass
				else:
					player_num = row[0].text.strip()
					player_name = row[1].text.strip()
					player_name = player_name.replace('\n','')
					player_name = player_name.replace('                                                    ',' ')
					#print(player_name)
					if player_name != 'Totals' and player_name != 'Opponent':
						player_year = row[2].text.strip()
						player_position = row[3].text.strip()
						player_position = player_position.replace('CATCHER','C')
						player_url = "https://www.cccaasports.org" + str(row[1].find("a").get("href")) #
						#print(len(player_url.split('/')))
						try:
							player_id = player_url.split('/')[7]
						except:
							player_id = player_url.split('/')[6]

						row_df = pd.DataFrame({'No.':player_num,'Name':player_name,'Position':player_position,'Year':player_year,'PlayerURL':player_url,'PlayerID':player_id},index=[0])
						roster_df = pd.concat([roster_df,row_df],ignore_index=True)
						del row_df
				count += 1

			roster_df['team_name'] = team_name
			roster_df['team_cccaa_season'] = team_cccaa_season
			#roster_df['team_njcaa_division'] = team_njcaa_division
			roster_df['team_id'] = team_id
			roster_df['team_season'] = team_season
			roster_df.to_csv(f'rosters/team_rosters/{team_cccaa_season}_{team_id}.csv',index=False)
			print(roster_df)
		except:
			print(f"Could not find a roster for the {team_cccaa_season} {team_name} baseball team.")
		
		time.sleep(5)

def main():
	get_CCCAA_Rosters()

if __name__ == "__main__":
	main()
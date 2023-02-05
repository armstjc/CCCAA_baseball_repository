import pandas as pd
import numpy as np
import datetime

## L
def generate_league_batting_stats(save=False):
    main_df = pd.DataFrame()
    s_df = pd.DataFrame()
    for season in range(2012,2024):
        s_df = pd.read_parquet(f'game_stats/player/batting_game_stats/parquet/{season}_batting.parquet')
        main_df = pd.concat([main_df,s_df],ignore_index=True)

    finished_df = pd.DataFrame(main_df.groupby(['season','cccaa_season'],as_index=False)['PA','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','TB','HBP','SH','SF','XBH','HDP','GO','FO'].sum())
    ## Groundouts/Flyouts ratio
    finished_df['GO/FO'] = finished_df['GO'] / finished_df['FO']
    ## Batting Average
    finished_df['BA'] = finished_df['H'] / finished_df['AB']
    ## On Base Percentage (OBP)
    finished_df['OBP'] = (finished_df['H'] + finished_df['BB'] + finished_df['HBP']) / finished_df['PA']
    ## Slugging Percentae
    finished_df['SLG'] = (finished_df['H'] + (finished_df['2B'] * 2) + (finished_df['3B'] * 3) + (finished_df['HR'] * 4)) / finished_df['AB']
    ## On-Base + Slugging Percentages
    finished_df['OPS'] = finished_df['OBP'] + finished_df['SLG']
    ## Batting Average on balls in play
    finished_df['BAbip'] = (finished_df['H'] - finished_df['HR']) / (finished_df['AB'] - finished_df['K'] - finished_df['HR'] + finished_df['SF'])
    ## Runs scored percentage
    finished_df['RS%'] = (finished_df['R'] - finished_df['HR']) / (finished_df['H'] + finished_df['HBP'] + finished_df['BB'] - finished_df['HR'])
    ## Home Run percentage
    finished_df['HR%'] = finished_df['HR'] / finished_df['PA']
    ## Strikeout percentage
    finished_df['K%'] = finished_df['K'] / finished_df['PA']
    ## Strikeout percentage
    finished_df['BB%'] = finished_df['BB'] / finished_df['PA']
    ## Walks to strikeouts ratio
    finished_df['K-BB%'] = finished_df['K%'] - finished_df['BB%']
    finished_df['BB/K'] = finished_df['BB'] / finished_df['K']
    ## Convert infinates into Null values
    #finished_df = finished_df.mask(np.isinf(finished_df))
    finished_df.replace([np.inf, -np.inf],np.nan,inplace=True)
    print(finished_df)

    if save == True:
        finished_df.to_csv(f'season_stats/league/batting_season_stats/csv/league_batting_stats.csv',index=False)
        finished_df.to_parquet(f'season_stats/league/batting_season_stats/parquet/league_batting_stats.parquet',index=False)

    return finished_df

def generate_league_pitching_stats(save=False):

    main_df = pd.DataFrame()
    s_df = pd.DataFrame()
    for season in range(2012,2024):
        s_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{season}_pitching.parquet')
        main_df = pd.concat([main_df,s_df],ignore_index=True)

    main_df = main_df.dropna(subset=['IP'])
    main_df = main_df.astype({'IP':'string'})

    main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
    main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
    main_df['IP'] = round(main_df['whole_innings'] + (main_df['part_innings']/3),3)
    finished_df = pd.DataFrame(main_df.groupby(['season','cccaa_season'],as_index=False)['IP','H','R','ER','BB','K','HR'].sum())
    
    ## Earned Run Average (ERA)
    finished_df['ERA'] = 9 * (finished_df['ER'] / finished_df['IP'])
    ## Walks and Hits per Inning Pitched (WHIP)
    finished_df['WHIP'] = (finished_df['BB'] + finished_df['H']) / finished_df['IP']
    ## Hits per 9 innings
    finished_df['H9'] = 9 * (finished_df['H'] / finished_df['IP'])
    ## Home Runs per 9 innings
    finished_df['HR9'] = 9 * (finished_df['HR'] / finished_df['IP'])
    ## Walks per 9 innings
    finished_df['BB9'] = 9 * (finished_df['BB'] / finished_df['IP'])
    ## Strikeouts per 9 innings
    finished_df['K9'] = 9 * (finished_df['K'] / finished_df['IP'])
    ## Strikeouts/Walks ratio
    finished_df['K/BB'] = finished_df['K'] / finished_df['BB']
    ## Runs Allowed per 9 innings pitched (RA9)
    finished_df['RA9'] = 9 * (finished_df['R'] / finished_df['IP'])

    ## Convert infinates into Null values
    finished_df.replace([np.inf, -np.inf],np.nan,inplace=True)

    if save == True:
        finished_df.to_csv(f'season_stats/league/pitching_season_stats/csv/leauge_pitching.csv',index=False)
        finished_df.to_parquet(f'season_stats/league/pitching_season_stats/parquet/leauge_pitching.parquet',index=False)

    return finished_df

def generate_season_player_batting_stats(season:int,save=False):
    main_df = pd.read_parquet(f'game_stats/player/batting_game_stats/parquet/{season}_batting.parquet')
    main_df['G'] = 1

    league_df = pd.read_parquet(f'season_stats/league/batting_season_stats/parquet/league_batting_stats.parquet')
    league_df = league_df[league_df['season'] == season]
    lg_obs = league_df['OPS'].iloc[0]
    finished_df = pd.DataFrame(main_df.groupby(['season','cccaa_season','team','team_id','player_id','player_name'],as_index=False)['G','PA','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','K','TB','HBP','SH','SF','XBH','HDP','GO','FO'].sum())
    
    ## Groundouts/Flyouts ratio
    finished_df['GO/FO'] = finished_df['GO'] / finished_df['FO']
    ## Batting Average
    finished_df['BA'] = finished_df['H'] / finished_df['AB']
    ## On Base Percentage (OBP)
    finished_df['OBP'] = (finished_df['H'] + finished_df['BB'] + finished_df['HBP']) / finished_df['PA']
    ## Slugging Percentae
    finished_df['SLG'] = (finished_df['H'] + (finished_df['2B'] * 2) + (finished_df['3B'] * 3) + (finished_df['HR'] * 4)) / finished_df['AB']
    ## On-Base + Slugging Percentages
    finished_df['OPS'] = finished_df['OBP'] + finished_df['SLG']
    ## OPS+
    finished_df['OPS+'] = 100 * (finished_df['OPS'] / lg_obs)
    finished_df['OPS+'] = finished_df['OPS+'].round(0)
    ## Batting Average on balls in play
    finished_df['BAbip'] = (finished_df['H'] - finished_df['HR']) / (finished_df['AB'] - finished_df['K'] - finished_df['HR'] + finished_df['SF'])
    ## Runs scored percentage
    finished_df['RS%'] = (finished_df['R'] - finished_df['HR']) / (finished_df['H'] + finished_df['HBP'] + finished_df['BB'] - finished_df['HR'])
    ## Home Run percentage
    finished_df['HR%'] = finished_df['HR'] / finished_df['PA']
    ## Strikeout percentage
    finished_df['K%'] = finished_df['K'] / finished_df['PA']
    ## Strikeout percentage
    finished_df['BB%'] = finished_df['BB'] / finished_df['PA']
    ## Walks to strikeouts ratio
    finished_df['K-BB%'] = finished_df['K%'] - finished_df['BB%']
    finished_df['BB/K'] = finished_df['BB'] / finished_df['K']
    ## Convert infinates into Null values
    finished_df.replace([np.inf, -np.inf],np.nan,inplace=True)
    print(finished_df)
    
    if save == True:
        finished_df.to_csv(f'season_stats/player/batting_season_stats/csv/{season}_batting.csv',index=False)
        finished_df.to_parquet(f'season_stats/player/batting_season_stats/parquet/{season}_batting.parquet',index=False)

    return finished_df

def generate_season_player_pitching_stats(season:int,save=False):

    main_df = pd.read_parquet(f'game_stats/player/pitching_game_stats/parquet/{season}_pitching.parquet')
    main_df = main_df.dropna(subset=['IP'])
    main_df['App'] = 1
    main_df = main_df.astype({'IP':'string'})

    main_df[['whole_innings','part_innings']] = main_df['IP'].str.split('.',expand=True)
    main_df = main_df.astype({'whole_innings':'int','part_innings':'int'})
    main_df['IP'] = round(main_df['whole_innings'] + (main_df['part_innings']/3),3)
    finished_df = pd.DataFrame(main_df.groupby(['season','cccaa_season','team','team_id','player_id','player_name'],as_index=False)['App','GS','W','L','SV','IP','H','R','ER','BB','K','HR'].sum())
    
    ## Win-loss percentage
    finished_df['W-L%'] = finished_df['W'] / (finished_df['W'] + finished_df['L'])
    ## Earned Run Average (ERA)
    finished_df['ERA'] = 9 * (finished_df['ER'] / finished_df['IP'])
    ## Walks and Hits per Inning Pitched (WHIP)
    finished_df['WHIP'] = (finished_df['BB'] + finished_df['H']) / finished_df['IP']
    ## Hits per 9 innings
    finished_df['H9'] = 9 * (finished_df['H'] / finished_df['IP'])
    ## Home Runs per 9 innings
    finished_df['HR9'] = 9 * (finished_df['HR'] / finished_df['IP'])
    ## Walks per 9 innings
    finished_df['BB9'] = 9 * (finished_df['BB'] / finished_df['IP'])
    ## Strikeouts per 9 innings
    finished_df['K9'] = 9 * (finished_df['K'] / finished_df['IP'])
    ## Strikeouts/Walks ratio
    finished_df['K/BB'] = finished_df['K'] / finished_df['BB']
    ## Runs Allowed per 9 innings pitched (RA9)
    finished_df['RA9'] = 9 * (finished_df['R'] / finished_df['IP'])

    ## Convert infinates into Null values
    finished_df.replace([np.inf, -np.inf],np.nan,inplace=True)

    if save == True:
        finished_df.to_csv(f'season_stats/player/pitching_season_stats/csv/{season}_pitching.csv',index=False)
        finished_df.to_parquet(f'season_stats/player/pitching_season_stats/parquet/{season}_pitching.parquet',index=False)

    return finished_df

def main():
    print('Starting up')
    current_year = int(datetime.date.today().year)
    generate_league_batting_stats(True)
    generate_league_pitching_stats(True)

    for i in range(2012,current_year+1):
        generate_season_player_batting_stats(i,True)
        generate_season_player_pitching_stats(i,True)

if __name__ == "__main__":
    main()
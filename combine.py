import os
import glob
from tqdm import tqdm
import pandas as pd
import numpy as np
from multiprocessing import Pool

def reader(filename):    
    return pd.read_csv(filename, encoding='latin-1')

def mergeFilesMultithreaded(filePath=""):
    #global filecount
    #filecount = 0
    num_cpus = os.cpu_count()
    print(f'{num_cpus} cpu cores advalible to this script.')

    pool = Pool(num_cpus-1)
    main_df = pd.DataFrame()
    
    l = filePath
    file_list = glob.iglob(l+"/*csv")
    file_list = list(file_list)
    df_list = pool.map(reader,tqdm(file_list))

    main_df = pd.concat(df_list)

    return main_df

def mergeRosters():
    f = "rosters/team_rosters"
    df = mergeFilesMultithreaded(f)
    df = df.sort_values(by=["team_season","team_id"])
    df.to_csv("rosters/cccaa_rosters.csv",index=False)
    #df.to_parquet("rosters/cccaa_rosters.parquet",index=False)
    max_season = df['team_season'].max()
    min_season = df['team_season'].min()
    for i in range(min_season,max_season+1):
        s_df = df[df['team_season'] == i]
        s_df.to_csv(f"rosters/csv/{i}_rosters.csv",index=False)
        #s_df.to_parquet(f"rosters/csv/{i}_rosters.parquet")

def mergeBattingStats():
    f = f"player_stats/batting"
    df = mergeFilesMultithreaded(f)
    max_season = df['season'].max()
    min_season = df['season'].min()


    for i in range(min_season,max_season+1):

        s_df = df[df['season'] == i]
        s_df.to_parquet(f"game_stats/player/batting_game_stats/parquet/{i}_batting.parquet",index=False)

        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df:2*len_s_df]
        partThree = s_df.iloc[2*len_s_df:3*len_s_df]
        partFour = s_df.iloc[3*len_s_df:]

        partOne.to_csv(f'game_stats/player/batting_game_stats/csv/{i}_batting_01.csv',index=False)
        partTwo.to_csv(f'game_stats/player/batting_game_stats/csv/{i}_batting_02.csv',index=False)
        partThree.to_csv(f'game_stats/player/batting_game_stats/csv/{i}_batting_03.csv',index=False)
        partFour.to_csv(f'game_stats/player/batting_game_stats/csv/{i}_batting_04.csv',index=False)

def mergePitchingStats():
    f = f"player_stats/pitching"
    df = mergeFilesMultithreaded(f)
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        s_df = df[df['season'] == i]
        s_df.to_parquet(f"game_stats/player/pitching_game_stats/parquet/{i}_pitching.parquet")
        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df:2*len_s_df]
        partThree = s_df.iloc[2*len_s_df:3*len_s_df]
        partFour = s_df.iloc[3*len_s_df:]

        partOne.to_csv(f'game_stats/player/pitching_game_stats/csv/{i}_pitching_01.csv',index=False)
        partTwo.to_csv(f'game_stats/player/pitching_game_stats/csv/{i}_pitching_02.csv',index=False)
        partThree.to_csv(f'game_stats/player/pitching_game_stats/csv/{i}_pitching_03.csv',index=False)
        partFour.to_csv(f'game_stats/player/pitching_game_stats/csv/{i}_pitching_04.csv',index=False)

def mergeFieldingStats():
    f = f"player_stats/fielding"
    df = mergeFilesMultithreaded(f)
    max_season = df['season'].max()
    min_season = df['season'].min()
    for i in range(min_season,max_season+1):
        s_df = df[df['season'] == i]
        s_df.to_parquet(f"game_stats/player/fielding_game_stats/parquet/{i}_fielding.parquet")
        
        len_s_df = len(s_df)
        len_s_df = len_s_df // 4
        partOne = s_df.iloc[:len_s_df]
        partTwo = s_df.iloc[len_s_df:2*len_s_df]
        partThree = s_df.iloc[2*len_s_df:3*len_s_df]
        partFour = s_df.iloc[3*len_s_df:]

        partOne.to_csv(f'game_stats/player/fielding_game_stats/csv/{i}_fielding_01.csv',index=False)
        partTwo.to_csv(f'game_stats/player/fielding_game_stats/csv/{i}_fielding_02.csv',index=False)
        partThree.to_csv(f'game_stats/player/fielding_game_stats/csv/{i}_fielding_03.csv',index=False)
        partFour.to_csv(f'game_stats/player/fielding_game_stats/csv/{i}_fielding_04.csv',index=False)

def main():
    mergeRosters()

    mergeBattingStats()
    mergePitchingStats()
    mergeFieldingStats()

if __name__ == "__main__":
    main()
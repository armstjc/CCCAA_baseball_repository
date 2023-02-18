import pandas as pd

from get_cccaa_rosters import get_CCCAA_Rosters
from combine import *
from get_cccaa_game_stats import get_CCCAA_gamelogs
from generate_stats import generate_stats_main

def main():
    year = 2023
    
    get_CCCAA_Rosters(year)
    mergeRosters()
    
    df = pd.read_csv(f'rosters/csv/{year}_rosters.csv')
    get_CCCAA_gamelogs(df)

    mergeBattingStats()
    mergePitchingStats()
    mergeFieldingStats()

    generate_stats_main()
    
if __name__ == "__main__":
    main()

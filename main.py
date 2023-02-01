import pandas as pd

from get_cccaa_rosters import get_CCCAA_Rosters
from combine import *
from get_cccaa_game_stats import get_CCCAA_gamelogs

if __name__ == "__main__":
    year = 2023
    
    get_CCCAA_Rosters(2023)
    mergeRosters()
    
    df = pd.read_csv('rosters/csv/2023_rosters.csv')
    get_CCCAA_gamelogs(df)

    mergeBattingStats()
    mergePitchingStats()
    mergeFieldingStats()

import pandas as pd

from get_cccaa_rosters import get_CCCAA_Rosters
from combine import *
from get_cccaa_game_stats import get_CCCAA_gamelogs

def main():
    year = 2023
    
    get_CCCAA_Rosters(year)
    mergeRosters()
    
    df = pd.read_csv(f'rosters/csv/{year}_rosters.csv')
    get_CCCAA_gamelogs(df)

    mergeBattingStats()
    mergePitchingStats()
    mergeFieldingStats()

if __name__ == "__main__":
    main()

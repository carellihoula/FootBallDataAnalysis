import pandas as pd

# Preprocess data
def preprocess_data(df):
    df['HomeTeamGoals'] = df['FTHG']
    df['AwayTeamGoals'] = df['FTAG']
    df['HomeTeamWin'] = (df['FTR'] == 'H').astype(int)
    df['AwayTeamWin'] = (df['FTR'] == 'A').astype(int)
    df['GameTie'] = (df['FTR'] == 'D').astype(int)
    return df.drop(columns=['FTHG', 'FTAG', 'FTR'])

# Filter Bundesliga data
def filter_bundesliga(df):
    return df[(df['Div'] == 'D1') & (df['Season'] >= 2000) & (df['Season'] <= 2015)]

# Aggregate team stats
def aggregate_team_stats(df):
    df_home = df.groupby(['Season', 'HomeTeam']).agg(
        TotalHomeWin=('HomeTeamWin', 'sum'),
        TotalHomeLoss=('AwayTeamWin', 'sum'),
        TotalHomeTie=('GameTie', 'sum'),
        HomeScoredGoals=('HomeTeamGoals', 'sum'),
        HomeAgainstGoals=('AwayTeamGoals', 'sum')
    ).rename_axis(['Season', 'Team']).reset_index()

    df_away = df.groupby(['Season', 'AwayTeam']).agg(
        TotalAwayWin=('AwayTeamWin', 'sum'),
        TotalAwayLoss=('HomeTeamWin', 'sum'),
        TotalAwayTie=('GameTie', 'sum'),
        AwayScoredGoals=('AwayTeamGoals', 'sum'),
        AwayAgainstGoals=('HomeTeamGoals', 'sum')
    ).rename_axis(['Season', 'Team']).reset_index()

    df_merged = pd.merge(df_home, df_away, on=['Season', 'Team'])
    
    df_merged['GoalsScored'] = df_merged['HomeScoredGoals'] + df_merged['AwayScoredGoals']
    df_merged['GoalsAgainst'] = df_merged['HomeAgainstGoals'] + df_merged['AwayAgainstGoals']
    df_merged['Win'] = df_merged['TotalHomeWin'] + df_merged['TotalAwayWin']
    df_merged['Loss'] = df_merged['TotalHomeLoss'] + df_merged['TotalAwayLoss']
    df_merged['Tie'] = df_merged['TotalHomeTie'] + df_merged['TotalAwayTie']
    df_merged['total_matches'] = df_merged['Win'] + df_merged['Loss'] + df_merged['Tie']
    
    return df_merged.drop(columns=[
        'TotalHomeWin', 'TotalHomeLoss', 'TotalHomeTie', 'HomeScoredGoals', 'HomeAgainstGoals',
        'TotalAwayWin', 'TotalAwayLoss', 'TotalAwayTie', 'AwayScoredGoals', 'AwayAgainstGoals'
    ])

# Calculate additional metrics and rank teams
def calculate_additional_metrics(df):
    df['GoalDiff'] = df['GoalsScored'] - df['GoalsAgainst']
    df['winPercentage'] = (df['Win'] / df['total_matches'] * 100).round(2)
    df['TeamPosition'] = df.groupby('Season')['winPercentage'].rank(ascending=False, method='first').astype(int)
    return df

# Extract best teams
def get_best_teams(df):
    df_best_team = df[df['TeamPosition'] == 1]
    df_top_three = df[df['TeamPosition'] <= 3]
    return df_best_team, df_top_three

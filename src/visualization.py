import plotly.express as px


#Generate bar plots for the best teams in each season.  
def plot_best_teams(df_best_team):
   
    df_best_pd = df_best_team.toPandas()
    figures = []
    for season in df_best_pd['Season'].unique():
        season_data = df_best_pd[df_best_pd['Season'] == season]
        fig = px.bar(season_data, x='Team', y='winPercentage',
                     title=f'Best Teams in Season {season}',
                     labels={'winPercentage': 'Win Percentage', 'Team': 'Team'})
        figures.append(fig)
    return figures

#Generate bar plots for the top three teams in each season.
def plot_top_three_teams(df_top_three):
    df_top_three_pd = df_top_three.toPandas()
    figures = []
    for season in df_top_three_pd['Season'].unique():
        season_data = df_top_three_pd[df_top_three_pd['Season'] == season]
        fig = px.bar(season_data, x='Team', y='winPercentage',
                     title=f'Top 3 Teams in Season {season}',
                     labels={'winPercentage': 'Win Percentage', 'Team': 'Team'})
        figures.append(fig)
    return figures

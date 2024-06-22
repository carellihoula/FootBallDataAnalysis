import plotly.express as px
import streamlit as st

#Generate bar plots for the best teams in each season.  
def plot_team_performance(df_ranked):
    
    df_ranked_pd = df_ranked.toPandas()

    #Select a team to view its evolution over the seasons
    teams = df_ranked_pd["Team"].unique()
    selected_team = st.selectbox("select a team", teams)

    # Filter data for selected team
    team_data = df_ranked_pd[df_ranked_pd['Team'] == selected_team]
    fig_team = px.line(
    team_data, 
    x='Season', 
    y='winPercentage', 
    title=f'Performance of {selected_team} over the seasons', 
    labels={'winPercentage': 'Win Percentage', 'Season': 'Season'},
    hover_data={'TeamPosition': True}
    )
    return st.plotly_chart(fig_team)

#Display the ranking of teams for a selected season.
def display_season_ranking(df_ranked):
    """
    Args:
    df_ranked (DataFrame): DataFrame containing the performance data for all teams.
    
    Returns:
    st.table: Streamlit table object showing the team rankings.
    """
    df_ranked_pd = df_ranked.toPandas()

    # Select a season to view its rankings
    seasons = df_ranked_pd["Season"].unique()
    selected_season = st.selectbox("Select a season", seasons)

    # Filter data for selected season
    season_data = df_ranked_pd[df_ranked_pd['Season'] == selected_season].sort_values(by='TeamPosition')
    
   # Format the values to the required decimal places
    season_data['winPercentage'] = season_data['winPercentage'].apply(lambda x: f"{x:.2f}%")
    season_data['GoalDiff'] = season_data['GoalDiff'].apply(lambda x: f"{x:.0f}")
    season_data['GoalsScored'] = season_data['GoalsScored'].apply(lambda x: f"{x:.0f}")
    season_data['GoalsAgainst'] = season_data['GoalsAgainst'].apply(lambda x: f"{x:.0f}")

    # Display rankings as a table
    st.write(f"Team Rankings for Season {selected_season}")
    st.table(season_data[['TeamPosition', 'Team', 'winPercentage', "Win","Loss", "Tie", 'GoalDiff', 'GoalsScored',"GoalsAgainst",]].reset_index(drop=True))

    # Optionally, display a bar chart of the rankings
    fig_rankings = px.bar(
        season_data, 
        x='Team', 
        y='winPercentage', 
        title=f'Team Rankings for Season {selected_season}', 
        labels={'winPercentage': 'Win Percentage', 'Team': 'Team'},
        hover_data={'TeamPosition': True}
    )
    st.plotly_chart(fig_rankings)



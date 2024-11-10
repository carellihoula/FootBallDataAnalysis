import streamlit as st
import plotly.express as px

# Generate bar plots for the best teams in each season
def plot_team_performance(df_ranked):
    teams = df_ranked["Team"].unique()
    selected_team = st.selectbox("Select a team", teams)
    team_data = df_ranked[df_ranked['Team'] == selected_team]
    fig_team = px.line(
        team_data, 
        x='Season', 
        y='winPercentage', 
        title=f'Performance of {selected_team} over the seasons', 
        labels={'winPercentage': 'Win Percentage', 'Season': 'Season'},
        hover_data={'TeamPosition': True}
    )
    st.plotly_chart(fig_team)

# Display the ranking of teams for a selected season
def display_season_ranking(df_ranked):
    seasons = df_ranked["Season"].unique()
    selected_season = st.selectbox("Select a season", seasons)
    season_data = df_ranked[df_ranked['Season'] == selected_season].sort_values(by='TeamPosition')
    season_data['winPercentage'] = season_data['winPercentage'].apply(lambda x: f"{x:.2f}%")
    st.write(f"Team Rankings for Season {selected_season}")
    st.table(season_data[['TeamPosition', 'Team', 'winPercentage', "Win", "Loss", "Tie", 'GoalDiff', 'GoalsScored', "GoalsAgainst"]].reset_index(drop=True))
    fig_rankings = px.bar(
        season_data, 
        x='Team', 
        y='winPercentage', 
        title=f'Team Rankings for Season {selected_season}', 
        labels={'winPercentage': 'Win Percentage', 'Team': 'Team'},
        hover_data={'TeamPosition': True}
    )
    st.plotly_chart(fig_rankings)

import streamlit as st
import plotly.express as px
from src.data_processing import (load_config, create_spark_session, load_data, preprocess_data, 
                                 filter_bundesliga, aggregate_team_stats, calculate_additional_metrics, 
                                 get_best_teams)
from src.visualization import plot_best_teams, plot_top_three_teams

# Load configuration
config = load_config('config/config.yaml')

# Create Spark session
spark = create_spark_session()

# Load data
df_matches = load_data(spark, config['data_path'])


################################################## ETL Process ##################################################

# Extract
df_preprocessed = preprocess_data(df_matches)

# Transform
df_bundesliga = filter_bundesliga(df_preprocessed)
df_aggregated = aggregate_team_stats(df_bundesliga)
df_ranked = calculate_additional_metrics(df_aggregated)

# Load
df_best_team, df_top_three = get_best_teams(df_ranked)

# Generate figures
best_team_figures = plot_best_teams(df_best_team)
top_three_figures = plot_top_three_teams(df_top_three)

# Convert DataFrames Spark to Pandas
df_ranked_pd = df_ranked.toPandas()
df_top_three_pd = df_top_three.toPandas()

#Select a team to view its evolution over the seasons
teams = df_ranked_pd["Team"].unique()
selected_team = st.selectbox("select a team", teams)

# Filter data for selected team
team_data = df_ranked_pd[df_ranked_pd['Team'] == selected_team]

# Plot the performance of the selected team over multiple seasons
fig_team = px.line(team_data, x='Season', y='winPercentage', title=f'Performance of {selected_team} over the seasons', labels={'winPercentage': 'Win Percentage', 'Season': 'Season'})
st.plotly_chart(fig_team)

# Plot the curves of the top three teams in the latest season
latest_season = df_top_three_pd['Season'].max()
top_three_latest_season = df_top_three_pd[df_top_three_pd['Season'] == latest_season]

fig_top_three = px.line(top_three_latest_season, x='Season', y='winPercentage', color='Team', title=f'Top 3 teams of the {latest_season} season', labels={'winPercentage': 'Win Percentage', 'Season': 'Season'})
st.plotly_chart(fig_top_three)
   
   


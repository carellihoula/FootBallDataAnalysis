import streamlit as st
import plotly.express as px
from src.data_processing import (load_config, create_spark_session, load_data, preprocess_data, 
                                 filter_bundesliga, aggregate_team_stats, calculate_additional_metrics, 
                                 get_best_teams)
from src.visualization import plot_team_performance, display_season_ranking

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
#top_three_figures = plot_top_three_teams(df_top_three)


################################################## Generate figures ##################################################

# Plot the performance of the selected team over multiple seasons
plot_team_performance(df_ranked)

# Display the rankings for the selected season
display_season_ranking(df_ranked)


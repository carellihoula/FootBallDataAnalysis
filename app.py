import streamlit as st
import plotly.express as px
from src.data_processing import (load_config, create_spark_session, load_data, preprocess_data, 
                                 filter_bundesliga, aggregate_team_stats, calculate_additional_metrics, 
                                 get_best_teams, save_to_bigquery)
from src.visualization import plot_team_performance, display_season_ranking
from google.oauth2 import service_account

# Set page layout to wide
st.set_page_config(layout="wide")

# Custom CSS to set the width to 80%
st.markdown(
    """
    <style>
    .main .block-container {
        max-width: 70%;
        padding-top: 2rem;
        padding-right: 2rem;
        padding-left: 2rem;
        padding-bottom: 2rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
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
# Google Cloud project details
project_id = 'coral-melody-399118'
dataset_id = 'carel_lihoula'
table_id = f'{dataset_id}.team_rankings'

# Path to your service account key file
service_account_info = st.secrets["gcp_service_account"]

credentials = service_account.Credentials.from_service_account_info(service_account_info)

# Save df_ranked to BigQuery
save_to_bigquery(df_ranked.toPandas(), table_id, project_id, credentials)

# Streamlit UI
st.markdown("<h1 style='text-align: center; color: black;'>Bundesliga Team Performance Analysis Dashboard</h1>", unsafe_allow_html=True)
################################################## Generate figures ##################################################

# Plot the performance of the selected team over multiple seasons
plot_team_performance(df_ranked)

# Display the rankings for the selected season
display_season_ranking(df_ranked)


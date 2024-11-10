import streamlit as st
import yaml
from pyspark.sql import SparkSession
from data_processing import preprocess_data, filter_bundesliga, aggregate_team_stats, calculate_additional_metrics, get_best_teams
from visualization import plot_team_performance, display_season_ranking

# Set page layout to wide
st.set_page_config(layout="wide")

# Custom CSS to set the width to 70%
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
def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

config = load_config('config/config.yaml')

# Create Spark session
def create_spark_session():
    return SparkSession.builder.appName("Data Analysis").getOrCreate()

spark = create_spark_session()

# Load data
def load_data(spark, data_path):
    return spark.read.format('csv').options(header='True').load(data_path)

df_matches = load_data(spark, config['data_path'])

################################################## ETL Process ##################################################

# Preprocess and filter data
df_preprocessed = preprocess_data(df_matches)
df_bundesliga = filter_bundesliga(df_preprocessed)
df_aggregated = aggregate_team_stats(df_bundesliga)
df_ranked = calculate_additional_metrics(df_aggregated)
df_best_team, df_top_three = get_best_teams(df_ranked)

################################################## Streamlit UI ##################################################

st.markdown("<h1 style='text-align: center; color: black;'>Bundesliga Team Performance Analysis Dashboard</h1>", unsafe_allow_html=True)

# Call display functions
plot_team_performance(df_ranked)
display_season_ranking(df_ranked)

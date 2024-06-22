import streamlit as st
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

# Streamlit UI
st.title("Bundesliga Team Performance Analysis")

# Tabs
tab1, tab2 = st.tabs(["Best Teams by Season", "Top 3 Teams by Season"])

with tab1:
    for fig in best_team_figures:
        st.plotly_chart(fig)

with tab2:
    for fig in top_three_figures:
        st.plotly_chart(fig)

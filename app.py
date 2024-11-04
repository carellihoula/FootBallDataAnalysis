import streamlit as st
import plotly.express as px
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, round, rank
from pyspark.sql import Window
import pandas as pd

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

# Preprocess data
def preprocess_data(df):
    df = df.selectExpr("*", "`FTHG` AS `HomeTeamGoals`", "`FTAG` AS `AwayTeamGoals`", "`FTR` AS `FinalResult`")
    df = df.drop('FTHG', 'FTAG', 'FTR')
    df = df.withColumn("HomeTeamWin", when(col("FinalResult") == "H", 1).otherwise(0))\
           .withColumn("AwayTeamWin", when(col("FinalResult") == "A", 1).otherwise(0))\
           .withColumn("GameTie", when(col("FinalResult") == "D", 1).otherwise(0))
    return df

df_preprocessed = preprocess_data(df_matches)

# Filter Bundesliga data
def filter_bundesliga(df):
    return df.filter((col("Div") == "D1") & (col("Season") >= 2000) & (col("Season") <= 2015))

df_bundesliga = filter_bundesliga(df_preprocessed)

# Aggregate team stats
def aggregate_team_stats(df):
    df_home = df.groupby("Season", "HomeTeam").agg(
        sum("HomeTeamWin").alias("TotalHomeWin"),
        sum("AwayTeamWin").alias("TotalHomeLoss"),
        sum("GameTie").alias("TotalHomeTie"),
        sum("HomeTeamGoals").alias("HomeScoredGoals"),
        sum("AwayTeamGoals").alias("HomeAgainstGoals")
    ).withColumnRenamed("HomeTeam", "Team")

    df_away = df.groupby("Season", "AwayTeam").agg(
        sum("AwayTeamWin").alias("TotalAwayWin"),
        sum("HomeTeamWin").alias("TotalAwayLoss"),
        sum("GameTie").alias("TotalAwayTie"),
        sum("HomeTeamGoals").alias("AwayAgainstGoals"),
        sum("AwayTeamGoals").alias("AwayScoredGoals")
    ).withColumnRenamed("AwayTeam", "Team")

    df_merged = df_home.join(df_away, ["Season", "Team"], 'inner')

    df_totals = df_merged.withColumn("GoalsScored", col("HomeScoredGoals") + col("AwayScoredGoals"))\
                         .withColumn("GoalsAgainst", col("HomeAgainstGoals") + col("AwayAgainstGoals"))\
                         .withColumn("Win", col("TotalHomeWin") + col("TotalAwayWin"))\
                         .withColumn("Loss", col("TotalHomeLoss") + col("TotalAwayLoss"))\
                         .withColumn("Tie", col("TotalHomeTie") + col("TotalAwayTie"))\
                         .withColumn("total_matches", col("Win") + col("Loss") + col("Tie"))

    drop_columns = ["TotalHomeWin", "TotalHomeLoss", "TotalHomeTie", "HomeScoredGoals", "HomeAgainstGoals",
                    "TotalAwayWin", "TotalAwayLoss", "TotalAwayTie", "AwayAgainstGoals", "AwayScoredGoals"]
    df_cleaned = df_totals.drop(*drop_columns)
    return df_cleaned

df_aggregated = aggregate_team_stats(df_bundesliga)

# Calculate additional metrics and rank teams
def calculate_additional_metrics(df):
    df_processed = df.withColumn("GoalDiff", col("GoalsScored") - col("GoalsAgainst"))\
                     .withColumn("winPercentage", round((col("Win") * 100 / col("total_matches")), 2))

    window_partition = Window.partitionBy("Season").orderBy(col("winPercentage").desc(), col("GoalDiff").desc())
    df_ranked = df_processed.withColumn("TeamPosition", rank().over(window_partition))
    return df_ranked

df_ranked = calculate_additional_metrics(df_aggregated)

# Extract best teams
def get_best_teams(df):
    df_best_team = df.filter(col("TeamPosition") == 1)
    df_top_three = df.filter(col("TeamPosition") <= 3)
    return df_best_team, df_top_three

df_best_team, df_top_three = get_best_teams(df_ranked)

# Save the final DataFrame to a local CSV file
df_ranked.toPandas().to_csv("team_rankings.csv", index=False)

################################################## Streamlit UI ##################################################

st.markdown("<h1 style='text-align: center; color: black;'>Bundesliga Team Performance Analysis Dashboard</h1>", unsafe_allow_html=True)

# Plot the performance of the selected team over multiple seasons
# Implement plot_team_performance locally if it's a custom function
# plot_team_performance(df_ranked)

# Display the rankings for the selected season
# Implement display_season_ranking locally if it's a custom function
# display_season_ranking(df_ranked)

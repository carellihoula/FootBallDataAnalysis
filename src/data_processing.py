from pyspark import SparkConf
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, round, rank
from pyspark.sql import Window
from pandas_gbq import to_gbq, read_gbq


# Extract configuration from a yaml file
def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)



def create_spark_session():
    conf = SparkConf().set("spark.driver.memory", "1g").set("spark.executor.memory", "1g")
    return SparkSession.builder.appName("Data Analysis").config(conf=conf).getOrCreate()

# Extract data from the CSV file
def load_data(spark, data_path):
    return spark.read.format('csv').options(header='True').load(data_path)

# Transform the data by renaming columns and adding new columns for analysis
def preprocess_data(df):
    df = df.selectExpr("*", "`FTHG` AS `HomeTeamGoals`", "`FTAG` AS `AwayTeamGoals`", "`FTR` AS `FinalResult`")
    df = df.drop('FTHG', 'FTAG', 'FTR')
    df = df.withColumn("HomeTeamWin", when(col("FinalResult") == "H", 1).otherwise(0))\
           .withColumn("AwayTeamWin", when(col("FinalResult") == "A", 1).otherwise(0))\
           .withColumn("GameTie", when(col("FinalResult") == "D", 1).otherwise(0))
    return df

# Filter data for Bundesliga matches between 2000 and 2015
def filter_bundesliga(df):
    return df.filter((col("Div") == "D1") & (col("Season") >= 2000) & (col("Season") <= 2015))

# Aggregate statistics for home and away teams
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

# Calculate additional metrics and rank teams
def calculate_additional_metrics(df):
    df_processed = df.withColumn("GoalDiff", col("GoalsScored") - col("GoalsAgainst"))\
                     .withColumn("winPercentage", round((col("Win") * 100 / col("total_matches")), 2))

    window_partition = Window.partitionBy("Season").orderBy(col("winPercentage").desc(), col("GoalDiff").desc())
    df_ranked = df_processed.withColumn("TeamPosition", rank().over(window_partition))
    return df_ranked

# Extract the best teams and top three teams by season
def get_best_teams(df):
    df_best_team = df.filter(col("TeamPosition") == 1)
    df_top_three = df.filter(col("TeamPosition") <= 3)
    return df_best_team, df_top_three


#Save the DataFrame to a BigQuery table.
def save_to_bigquery(df, table_id, project_id, credentials):
    """
    Args:
    df (DataFrame): DataFrame to be saved.
    table_id (str): BigQuery table identifier in the format 'dataset.table'.
    project_id (str): Google Cloud project ID.
    credentials (service_account.Credentials): Google Cloud service account credentials.
    """
    to_gbq(df, table_id, project_id=project_id, credentials=credentials, if_exists='replace')

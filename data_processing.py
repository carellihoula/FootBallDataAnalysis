from pyspark.sql.functions import when, col, sum, round, rank
from pyspark.sql import Window

# Preprocess data
def preprocess_data(df):
    df = df.selectExpr("*", "`FTHG` AS `HomeTeamGoals`", "`FTAG` AS `AwayTeamGoals`", "`FTR` AS `FinalResult`")
    df = df.drop('FTHG', 'FTAG', 'FTR')
    df = df.withColumn("HomeTeamWin", when(col("FinalResult") == "H", 1).otherwise(0))\
           .withColumn("AwayTeamWin", when(col("FinalResult") == "A", 1).otherwise(0))\
           .withColumn("GameTie", when(col("FinalResult") == "D", 1).otherwise(0))
    return df

# Filter Bundesliga data
def filter_bundesliga(df):
    return df.filter((col("Div") == "D1") & (col("Season") >= 2000) & (col("Season") <= 2015))

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

# Calculate additional metrics and rank teams
def calculate_additional_metrics(df):
    df_processed = df.withColumn("GoalDiff", col("GoalsScored") - col("GoalsAgainst"))\
                     .withColumn("winPercentage", round((col("Win") * 100 / col("total_matches")), 2))

    window_partition = Window.partitionBy("Season").orderBy(col("winPercentage").desc(), col("GoalDiff").desc())
    df_ranked = df_processed.withColumn("TeamPosition", rank().over(window_partition))
    return df_ranked

# Extract best teams
def get_best_teams(df):
    df_best_team = df.filter(col("TeamPosition") == 1)
    df_top_three = df.filter(col("TeamPosition") <= 3)
    return df_best_team, df_top_three

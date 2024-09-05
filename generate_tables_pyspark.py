# -*- coding: utf-8 -*-

import sys
import datetime
import os
import traceback
sys.path.append(os.getcwd())
import json
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import date_format, count, col, from_unixtime, when, lit, countDistinct, sum, round, format_string, split, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.storagelevel import StorageLevel


class GenerateTablesJob():
    """
    Class to generate tables of Github events.
    """

    def __init__(self, sc, output_dir):

        self.spark = sc
        self.output_dir = output_dir
        

    def run(self):
        print("Python version: " + str(sys.version))
        
        ## Read the data
        events_df = self.read_gh_data(os.path.join(self.output_dir, "input_data"))
        
        # Build the user table
        self.__build_and_save_user_table(events_df)        
        self.__build_and_save_repo_table(events_df)
    
    def read_gh_data(self, path):
        """
        Read the GH Archive data from the specified path.
        
        :param path: Path to the GH Archive data.
        :return: DataFrame containing the GH Archive data.
        """
        # Read the data
        events_df = self.spark.read.json(path)
        
        return events_df
        
        
    def __build_and_save_user_table(self, events_df):
        user_schema = StructType([
            StructField("date", StringType(), nullable=True),
            StructField("user_id", IntegerType(), nullable=True),
            StructField("user_login", StringType(), nullable=True),
            StructField("starred_projects", IntegerType(), nullable=True),
            StructField("created_issues", IntegerType(), nullable=True),
            StructField("created_prs", IntegerType(), nullable=True)
        ])
        
        user_empty_df = self.spark.createDataFrame([], user_schema)
        
        user_aggregates = events_df.groupBy(
            date_format(col("created_at"), "yyyy-MM-dd").alias("date"),
            col("actor.id").alias("user_id"),
            col("actor.login").alias("user_login")
            ).agg(
            count(when(col("type") == "WatchEvent", True)).alias("projects_starred").cast("int"),
            count(when(col("type") == "IssuesEvent", True)).alias("issues_created").cast("int"),
            count(when(col("type") == "PullRequestEvent", True)).alias("prs_created").cast("int")
        )

        user_final_df = user_empty_df.union(user_aggregates)
        
        self.__save_table(user_final_df, "user_aggregates")
        
    
    def __build_and_save_repo_table(self, events_df):
        # Define the schema
        repo_schema = StructType([
            StructField("date", StringType(), nullable=True),
            StructField("project_id", IntegerType(), nullable=True),
            StructField("project_name", StringType(), nullable=True),
            StructField("stars", IntegerType(), nullable=True),
            StructField("forks", IntegerType(), nullable=True),
            StructField("issues_created", IntegerType(), nullable=True),
            StructField("prs_created", IntegerType(), nullable=True)
        ])
        
        # Create an empty DataFrame with the specified schema
        repo_empty_df = self.spark.createDataFrame([], repo_schema)
        
        repo_aggregates = events_df.groupBy(
            date_format(col("created_at"), "yyyy-MM-dd").alias("date"),
            col("repo.id").alias("project_id"),
            col("repo.name").alias("project_name")
            ).agg(
            count(when(col("type") == "WatchEvent", True)).alias("stars").cast("int"), 
            count(when(col("type") == "ForkEvent", True)).alias("forks").cast("int"),
            count(when(col("type") == "IssuesEvent", True)).alias("issues_created").cast("int"),
            count(when(col("type") == "PullRequestEvent", True)).alias("prs_created").cast("int")
        )
        
        repo_final_df = repo_empty_df.union(repo_aggregates)
        
        self.__save_table(repo_final_df, "repository_aggregates")
        
    def __save_table(self, df, name):
        """
        Save the DataFrame as a CSV and Parquet file.
        
        :param df: DataFrame to save.
        :param name: Name of the table.
        """
        # Save as CSV
        df.coalesce(1) \
            .write.mode('overwrite') \
            .option("header", "true") \
            .option("encoding", "utf-8") \
            .option("sep", ',') \
            .option("compression", "gzip") \
            .csv(os.path.join(self.output_dir, f"{name}.csv"))
        
        # Save as Parquet
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(os.path.join(self.output_dir, f"{name}.parquet"))
        
        # Save as Delta Lake
        df.write.format("delta").save(os.path.join(self.output_dir, f"{name}.delta"))
        

    
if __name__ == "__main__":
    
    output_dir = sys.argv[1]
    
    try:
        # Create the Spark session
        conf = SparkConf() \
            .setAppName("Generate Tables of Github Events")
        
        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        job = GenerateTablesJob(spark, output_dir)
        job.run()
        
    except Exception as e:
        print(f"Error initializing Spark or running job: {e}")
        traceback.print_exc()
    
    finally:
        spark.stop()
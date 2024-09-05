import os
import pytest
import luigi
import requests
import sys
sys.path.append(os.getcwd())
from unittest import mock
from unittest.mock import patch

from datetime import datetime, timedelta
from luigi import LocalTarget
from download_gharchive import GHArchiveRetrieveData
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql import Row
from generate_tables_pyspark import GenerateTablesJob


@pytest.fixture(scope="module")
def spark():
    """
    Fixture to initialize a Spark session for testing.
    """
    spark = SparkSession.builder \
        .appName("PySpark Test") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_user_schema_validation(spark):
    """
    Test that the resulting DataFrame has the correct schema.
    """
    # Mock input DataFrame
    data = [
        {"created_at": "2023-09-05T12:00:00Z", "actor": {"id": 1, "login": "user1"}, "type": "WatchEvent"},
    ]
    
    events_df = spark.createDataFrame(data, StructType([
        StructField("created_at", StringType(), True),
        StructField("actor", StructType([
            StructField("id", IntegerType(), True),
            StructField("login", StringType(), True)
        ]), True),
        StructField("type", StringType(), True)
    ]))

    # Initialize the job
    job = GenerateTablesJob(spark, "/tmp")
    
    # Patch the __save_table method
    with patch.object(job, '_GenerateTablesJob__save_table') as mock_save_table:
        job._GenerateTablesJob__build_and_save_user_table(events_df)
        
        # Extract the argument passed to __save_table
        user_df = mock_save_table.call_args[0][0]
        
        # Check schema
        expected_schema = StructType([
            StructField("date", StringType(), nullable=True),
            StructField("user_id", IntegerType(), nullable=True),
            StructField("user_login", StringType(), nullable=True),
            StructField("starred_projects", IntegerType(), nullable=True),
            StructField("created_issues", IntegerType(), nullable=True),
            StructField("created_prs", IntegerType(), nullable=True)
        ])
        assert user_df.schema == expected_schema


def test_repo_schema_validation(spark):
    """
    Test that the resulting DataFrame has the correct schema for repository aggregates.
    """
    # Mock input DataFrame
    data = [
        {"created_at": "2023-09-05T12:00:00Z", "repo": {"id": 1, "name": "repo1"}, "type": "WatchEvent"},
        {"created_at": "2023-09-06T14:00:00Z", "repo": {"id": 2, "name": "repo2"}, "type": "ForkEvent"},
    ]
    
    events_df = spark.createDataFrame(data, StructType([
        StructField("created_at", StringType(), True),
        StructField("repo", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("type", StringType(), True)
    ]))

    # Initialize the job
    job = GenerateTablesJob(spark, "/tmp")
    
    # Patch the __save_table method
    with patch.object(job, '_GenerateTablesJob__save_table') as mock_save_table:
        job._GenerateTablesJob__build_and_save_repo_table(events_df)
        
        # Extract the argument passed to __save_table
        repo_df = mock_save_table.call_args[0][0]
        
        # Check schema
        expected_schema = StructType([
            StructField("date", StringType(), nullable=True),
            StructField("project_id", IntegerType(), nullable=True),
            StructField("project_name", StringType(), nullable=True),
            StructField("stars", IntegerType(), nullable=True),
            StructField("forks", IntegerType(), nullable=True),
            StructField("issues_created", IntegerType(), nullable=True),
            StructField("prs_created", IntegerType(), nullable=True)
        ])
        assert repo_df.schema == expected_schema




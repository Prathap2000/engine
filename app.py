import os
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import pandas as pd
import tempfile

# Function to initialize Spark session
def initialize_spark(gcs_bucket_url, gcp_access_key, iceberg_catalog):
    iceberg_jar_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "iceberg-spark-runtime-3.5_2.12-1.7.0.jar")
    gcs_connector_jar_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gcs-connector-hadoop2-latest.jar")

    spark = SparkSession.builder \
        .appName("Iceberg Query Engine") \
        .config("spark.sql.catalog." + iceberg_catalog, "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog." + iceberg_catalog + ".type", "hadoop") \
        .config("spark.sql.catalog." + iceberg_catalog + ".warehouse", f"gs://{gcs_bucket_url}/warehouse") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", gcp_access_key) \
        .config("spark.jars", iceberg_jar_path + "," + gcs_connector_jar_path) \
        .getOrCreate()

    return spark

# Function to fetch the latest snapshot data and remove duplicates
def filter_latest_data(iceberg_df, iceberg_catalog, iceberg_database, table_name, spark):
    try:
        # Get the history of the table
        history_df = spark.sql(f"SELECT * FROM {iceberg_catalog}.{iceberg_database}.{table_name}.history")
        
        # Assuming there is a 'timestamp' or 'commit_time' column in the history table to order by
        latest_snapshot = history_df.orderBy("timestamp", ascending=False).limit(1)
        latest_snapshot_id = latest_snapshot.collect()[0]["snapshot_id"]
        
        # Filter the Iceberg DataFrame by the latest snapshot ID
        iceberg_df = iceberg_df.filter(col("snapshot_id") == latest_snapshot_id)

        # Remove duplicates based on a primary key (assumed to be "id" in this case)
        # Replace "id" with your actual primary key column
        window_spec = Window.partitionBy("id").orderBy(col("timestamp").desc())
        iceberg_df = iceberg_df.withColumn("row_number", row_number().over(window_spec)) \
                               .filter(col("row_number") == 1) \
                               .drop("row_number")
        
        return iceberg_df
    except Exception as e:
        st.error(f"Error fetching snapshot data: {str(e)}")
        return iceberg_df  # Return the original DataFrame in case of error

# Function to run queries against Iceberg tables stored in GCS
def run_query(spark, query):
    try:
        # Run the query on Iceberg tables
        df = spark.sql(query)
        return df
    except Exception as e:
        return str(e)

# Function to handle query execution
def execute_query(spark, query, progress_bar):
    # Show progress bar
    with progress_bar:
        progress_bar.progress(0)
        try:
            # Run the query
            result = run_query(spark, query)
            progress_bar.progress(50)

            # Check if the result is a DataFrame (successful query) or an error message
            if isinstance(result, str):  # If there's an error
                st.error(f"Error running query: {result}")
                return None
            else:
                # Convert Spark DataFrame to Pandas DataFrame for better handling in the UI
                pandas_df = result.toPandas()

                # If no results, show a message
                if pandas_df.empty:
                    st.info("No results returned for the query.")
                    return None

                # Return the Pandas DataFrame
                progress_bar.progress(100)
                return pandas_df
        except Exception as e:
            st.error(f"Error executing the query: {str(e)}")
            return None

# Function to display results in a table format using Streamlit
def display_results(df):
    if df is not None:
        st.dataframe(df)

# Main function to set up the Streamlit app
def main():
    # User inputs for GCS bucket, GCP JSON key, and Iceberg catalog
    st.title("Iceberg Query Engine")
    
    iceberg_catalog = st.text_input("Enter your Iceberg catalog name:", "ice")
    gcs_bucket_url = st.text_input("Enter your GCS bucket URL:", "iceberg-storage")
    gcp_access_key = st.file_uploader("Upload your GCP service account JSON key:", type=["json"])
    

    if gcs_bucket_url and gcp_access_key and iceberg_catalog:
        # Use tempfile to create a temp directory for the JSON key on all platforms
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            gcp_access_key_path = tmp_file.name
            tmp_file.write(gcp_access_key.getbuffer())

        # Initialize Spark session with the user-provided credentials and catalog name
        spark = initialize_spark(gcs_bucket_url, gcp_access_key_path, iceberg_catalog)

        # SQL query input from the user
        query = st.text_area("Enter your SQL Query:")

        if st.button("Run Query"):
            # Progress bar
            progress_bar = st.progress(0)

            # Execute the query and get results
            pandas_df = execute_query(spark, query, progress_bar)

            # Display the results
            display_results(pandas_df)

            # Stop Spark session after query execution
            spark.stop()

if __name__ == "__main__":
    main()

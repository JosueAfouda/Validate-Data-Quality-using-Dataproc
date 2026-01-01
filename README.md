Dead letter queue (DLQ)

A DLQ is a standard concept in data engineering. It’s a mechanism for handling bad data without stopping your entire pipeline. When a record fails a validation check (e.g., a missing ID, a negative price), instead of throwing an error and crashing the job, you route that "dead" record to a separate location (the "queue" or "letter" file) for later inspection.

The video below is a quick look at how Cymbal Superstore manages data validation using DLQs and how this is implemented in Dataflow and Serverless for Apache Spark.

https://youtu.be/b7MXY2mxOoM?si=uIs59bfzLFaIrdCe

https://www.skills.google/paths/16/course_templates/53/html_bundles/592803


# Validate Data Quality for a Batch Data Pipeline using Serverless for Apache Spark 


## Overview

Serverless for Apache Spark from Google Cloud is a fully-managed service that simplifies running Spark batch workloads without managing infrastructure. This pattern provides a robust approach for ETL (Extract, Transform, Load) workflows, ensuring that only high-quality data lands in your analytical systems.

## The challenge

Raw data ingested into a data lake often contains imperfections such as missing values, incorrect formats, or invalid entries. Loading this data directly into an analytical warehouse can corrupt reports and lead to poor business decisions.

## The solution

Create an automated data quality pipeline. This pipeline intercepts raw data, applies a set of validation rules, and then intelligently routes the data. Clean records are sent to the production data warehouse, while records that fail validation are sent to a "dead-letter queue" (DLQ) for inspection and remediation.

In this lab, you will build this solution by running a custom PySpark job on Serverless for Apache Spark. The job will:

    Read a raw CSV file from a Cloud Storage bucket.
    
    Apply data quality rules to validate each record.
    
    Load the clean, valid records into a BigQuery table.
    
    Write the invalid records to a separate DLQ bucket in Cloud Storage.

This pattern ensures that your data warehouse remains pristine and provides a clear, auditable process for handling data errors.

## Enterprise use cases

    E-commerce: A pipeline validates incoming order data, ensuring product IDs are valid and customer emails are correctly formatted before loading into a sales analytics BigQuery table. Invalid orders are routed to a DLQ for manual review.
    
    Healthcare: A system processes patient records, validating that medical codes exist and dates are in the correct format. Records with errors are sent to a secure DLQ bucket for data stewardship review to ensure compliance.
    
    Finance: A daily pipeline ingests stock market data, checking for null values in critical fields like close_price. Incomplete ticker data is sent to a DLQ, preventing corruption of time-series analysis models.

## Objectives

In this lab, you will learn how to:

    Explore the pre-configured lab environment provisioned by Terraform.
    
    Complete the environment setup by creating a BigQuery dataset.
    
    Write a commented, custom PySpark script with data quality and routing logic.
    
    Configure and execute a Spark batch job on a custom, secure VPC network.
    
    Verify the clean data output in a BigQuery table.
    
    Review the invalid records in the Cloud Storage DLQ.

## Activate Cloud Shell 

- List the active account name:

gcloud auth list

- List the project ID:

gcloud config list project


## Your Lab Environment

When you begin this lab, a Terraform script runs to automatically provision most of the necessary infrastructure and resources. The following have been created for you:

    Project ID = qwiklabs-gcp-00-f7f32780700e

    Region = us-central1

    Zone = us-central1-c

    A custom VPC Network (spark-network) and Subnet (spark-subnet) configured with the required network access for Serverless for Apache Spark.

    Two Cloud Storage buckets:
    
        A main bucket (gs://qwiklabs-gcp-00-f7f32780700e-main-bucket) used for storing your PySpark script (scripts/), the raw input data (source/), and as a temporary staging area for the BigQuery connector.
        
        A DLQ bucket (gs://qwiklabs-gcp-00-f7f32780700e-dlq-bucket) dedicated to storing invalid records.

    A raw data file: A Python script has automatically generated and uploaded a 1,000-record CSV file (source/customer_contacts_1000.csv) to the main bucket. Approximately 20% of these records contain intentional imperfections (e.g., missing IDs, invalid emails) to test your pipeline.

Your goal is to write a script that can identify and separate the good records from the bad ones and then load them into the correct destinations.
Note on Bucket Strategy:
For simplicity in this lab, the main bucket is used for scripts, source data, and BigQuery temporary staging. In production environments, it is a best practice to use three separate buckets: one for raw/input data, one for the dead-letter queue, and a dedicated bucket for temporary staging data used by connectors like the BigQuery connector. This provides better isolation, security, and lifecycle management. 

Task 1. Explore the Environment and Prepare Services

First, you'll confirm that the lab resources were created correctly and preview the source data you will be working with.
Confirm the Cloud Storage buckets

    In the Google Cloud Console, use the Navigation menu (☰) to go to Cloud Storage > Buckets.
    Confirm that you see two buckets listed: one ending in -main-bucket and another ending in -dlq-bucket.

Preview the raw data and enable the API

    Activate Cloud Shell.

    Run the following command to view the header and the first 10 records of the raw CSV file located in your main bucket.

    gsutil cat gs://qwiklabs-gcp-00-f7f32780700e-main-bucket/source/customer_contacts_1000.csv | head -n 11

    Copied!

    Before you can run a job, you must enable the Dataproc API. Run the following command in Cloud Shell to enable it:

    gcloud services enable dataproc.googleapis.com

    Copied!

Click Check my progress to verify your performed task.

Assessment Completed!

Enable the Dataproc API.
Assessment Completed!
Task 2. Prepare the BigQuery Environment

Your Terraform script has set up the network and storage, but you still need to create the destination BigQuery dataset where your clean data will be loaded.
Create the dataset

    In Cloud Shell, run the following command to create a new BigQuery dataset named customer_data_clean.

    bq mk customer_data_clean

    Copied!

    You can now validate that the dataset was created successfully in the console. Use the Navigation Menu (☰) to go to BigQuery. In the Classic explorer panel, click the arrow next to your project ID to expand its contents, and you should see your new customer_data_clean dataset.

Click Check my progress to verify your performed task.

Assessment completed!

Create a BigQuery dataset.
Assessment completed!
Task 3. Prepare the PySpark Data Quality Script

Now, create the custom PySpark script that contains the logic for validating the data. The script's logic is straightforward:

    it reads the source CSV file from Cloud Storage into a DataFrame,
    applies a series of validation rules to check for null IDs and valid email formats,
    splits the DataFrame into two—one with clean records and one with invalid records, and finally,
    writes the clean data to BigQuery and the invalid data to the DLQ bucket in Cloud Storage.

Write and upload the script

    In Cloud Shell, create the PySpark script file named customer_dq.py.

    nano customer_dq.py

    Copied!

    Paste the following commented Python code into the nano editor.

    # Import necessary libraries

    import sys
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when

    # This script expects 1 command-line argument:
    # 1. The destination BigQuery table path in format 'dataset.table'

    if len(sys.argv) != 2:
        print("Usage: customer_dq.py <bq_dataset_table>")
        sys.exit(-1)

    # Assign command-line argument to variable
    bq_dataset_table = sys.argv[1]

    # Lab variables are substituted here when the lab runs
    bq_project = "qwiklabs-gcp-00-f7f32780700e"
    gcs_source_path = f"gs://{bq_project}-main-bucket/source/customer_contacts_1000.csv"
    gcs_dlq_path = f"gs://{bq_project}-dlq-bucket/errors/"

    # Initialize a new Spark Session
    spark = SparkSession.builder.appName("Customer DQ Check").getOrCreate()

    # Step 1: Read the source CSV data from the GCS bucket
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(gcs_source_path)

    # Step 2: Define the Data Quality rules
    # Rule 1: The 'id' column must not be null.
    dq_rule_id = col("id").isNotNull()

    # Rule 2: The 'email' column must not be null and must match a valid email format regex.
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    dq_rule_email = col("email").isNotNull().__and__(col("email").rlike(email_regex))

    # Step 3: Apply rules and split the DataFrame into clean and error records
    df_with_dq = df.withColumn("dq_passed", when(dq_rule_id.__and__(dq_rule_email), True).otherwise(False))
    clean_df = df_with_dq.filter(col("dq_passed") == True).drop("dq_passed")
    error_df = df_with_dq.filter(col("dq_passed") == False).drop("dq_passed")

    # Step 4: Write the clean records to the specified BigQuery table
    # The BigQuery connector requires a temporary GCS bucket NAME.
    temp_gcs_bucket_name = f"{bq_project}-main-bucket"

    clean_df.write \
        .format("bigquery") \
        .option("table", bq_dataset_table) \
        .option("temporaryGcsBucket", temp_gcs_bucket_name) \
        .option("project", bq_project) \
        .mode("overwrite") \
        .save()

    # Step 5: Write the error records to the DLQ bucket in GCS as a single CSV file
    error_df.repartition(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(gcs_dlq_path)

    # Stop the Spark session
    spark.stop()

    Copied!

Critical Note Ensure the final line of the script is spark.stop(). Delete anything that is below it, such as </bq_dataset_table>.

    Press CTRL+X, then Y, and then Enter to save and exit nano.

    Upload your new PySpark script to the main Cloud Storage bucket.


      # The command below uploads the script to a 'scripts' folder in the main data bucket
      gcloud storage cp customer_dq.py gs://qwiklabs-gcp-00-f7f32780700e-main-bucket/scripts/

    Copied!

Click Check my progress to verify your performed task.

Assessment Completed!

Prepare the PySpark Data Quality Script.
Assessment Completed!
Task 4. Configure and Run the Batch Pipeline

With the script uploaded, you can now configure and submit the job to Serverless for Apache Spark.
Execute the Spark job

1.  Set the following environment variables in Cloud Shell. These variables create shortcuts to the resources provisioned by Terraform.


    # The name for the final table in BigQuery
    export BQ_TABLE="valid_customers"

    # The BigQuery table path in 'dataset.table' format
    export BQ_DATASET_TABLE="customer_data_clean.${BQ_TABLE}"

    # The path to the 1000-record source CSV file
    export GCS_SOURCE_PATH="gs://qwiklabs-gcp-00-f7f32780700e-main-bucket/source/customer_contacts_1000.csv"

    # The GCS path where error records will be written
    export GCS_DLQ_PATH="gs://qwiklabs-gcp-00-f7f32780700e-dlq-bucket/errors/"

    # The GCS path to the PySpark script you just uploaded
    export PYSPARK_SCRIPT_PATH="gs://qwiklabs-gcp-00-f7f32780700e-main-bucket/scripts/customer_dq.py"

    # The full URI of the custom subnet created by Terraform
    export SUBNET_URI="projects/qwiklabs-gcp-00-f7f32780700e/regions/us-central1/subnetworks/spark-subnet"

Copied!

    Review the command below before running it. It submits your script as a batch job and passes your environment variables as arguments.
        --subnet: This flag is critical. It tells the job to run within the secure, custom spark-subnet created by Terraform, which is a security best practice.
        --deps-bucket: This flag specifies a GCS bucket for staging job dependencies.
        --: This double-dash separates the gcloud command's flags from the arguments that will be passed directly to your PySpark script.

    Run the command to submit the job:


     gcloud dataproc batches submit pyspark $PYSPARK_SCRIPT_PATH \
         --version=2.1 \
         --batch="customer-dq-job-$(date +%s)" \
         --region=us-central1 \
         --subnet=$SUBNET_URI \
         --deps-bucket=gs://qwiklabs-gcp-00-f7f32780700e-main-bucket \
         -- \
         $BQ_DATASET_TABLE

    Copied!

Note: The job will take 3-5 minutes to complete. You can monitor its progress in the Google Cloud Console by navigating to Dataproc > Serverless > Batches.

Click Check my progress to verify your performed task.

Please create and run the Batch Pipeline and wait for a few minutes to get a green check.

Run the Batch Pipeline.
Please create and run the Batch Pipeline and wait for a few minutes to get a green check.
Task 5. Verify the Clean Data in BigQuery

Now that the pipeline has run, verify that only the clean records were loaded into BigQuery.
Query the results table

    In Cloud Shell, run a query to count the clean records in the BigQuery table. The count should be approximately 800.

    bq query \
      --use_legacy_sql=false \
      'SELECT count(*) as total_clean_records FROM `customer_data_clean.valid_customers`;'

    Copied!

    To view a sample of the clean data, run the following command. The output will show records that all have valid IDs and email formats.

    bq query \
      --use_legacy_sql=false \
      'SELECT * FROM `customer_data_clean.valid_customers` LIMIT 10;'

    Copied!

Click Check my progress to verify your performed task.

Assessment Completed!

Verify the Data in BigQuery.
Assessment Completed!
Task 6. Review the Invalid Records in the DLQ

Finally, verify that the records that failed the data quality checks were correctly routed to the DLQ bucket for later analysis.
Inspect the error files via Cloud Shell

    In Cloud Shell, view a sample of the invalid records in the DLQ bucket. The head -n 11 command will show the header row plus the first 10 error records.

    gcloud storage cat gs://qwiklabs-gcp-00-f7f32780700e-dlq-bucket/errors/*.csv | head -n 11

    Copied!

    The command should return a sample of the approximately 200 records that failed validation. You will see rows with missing IDs or malformed emails.

    Example output:

    id,first_name,last_name,email
    ,Isabella,Smith,<REDACTED_EMAIL>
    12,Michael,Johnson,
    21,Sophia,Williams,sophia.williams@example

(Optional) Inspect the error files via the console

You can also view the error file directly in the Google Cloud Console:

    In the Navigation Menu (☰), go to Cloud Storage > Buckets.
    Click on the name of the bucket ending in -dlq-bucket.
    Navigate into the errors/ folder.
    Click on the name of the .csv file to open and view its contents in the browser.

Congratulations!

You have successfully built and tested a production-grade, batch data quality pipeline!

In this lab, you wrote a custom PySpark job to validate and process a file from Cloud Storage, loaded the clean results into a BigQuery table, and routed the invalid records to a DLQ bucket, all within a pre-provisioned, secure network environment. This pattern is a foundational component of modern, reliable data platforms.


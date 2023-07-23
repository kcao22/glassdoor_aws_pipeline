# Glassdoor Webscraping End-to-End AWS Pipeline
**Full pipeline process**:
![Alt Text](https://github.com/kcao22/glassdoor_aws_pipeline/blob/main/Data%20Modeling/KCPipeline.png)

**Demo of serving layer PowerBI Dashboard**:
![Alt Text](https://github.com/kcao22/glassdoor_aws_pipeline/blob/main/ReadMe%20Images/glassdoor_job_posts_dashboard.gif)

## Credits
 - For data on cities and states:
     - https://github.com/OpportunityInsights/EconomicTracker/tree/main

## Project Goal
 - To explore, design, and implement a data lakehouse using AWS Cloud Computing Services.
 - Create an automated pipeline beginning from data collection to automated ingestion, transformation, and serving.
 - Follow data governance and best practices during and after development of end-to-end pipeline.
   
## Libraries and Resources Used
 - **Python Version**: 3.10
 - **Main Packages**: boto3, Selenium
 - **Resources**: AWS Cloud Computing Services

# Pipeline Architecture
## Source Data, Extraction, and Storage
 1. Windows Task Scheduler is used to run daily web scraping operations at specified times. In doing so, jobs collected are only those posted in the last 24 hours and mitigates duplicate data posts.
 2. Using appropriate IAM role management and permissions, boto3 is used with appropriate credentials to post scraped data to Lake Formation buckets.
 4. Lambda triggers are built in to trigger upon posting of specific objects within S3 buckets, thereby initiating a cascade of services to read and write data to and from appropriate locations. This triggers the end-to-end automated pipeline.
 5. S3 object storage is chosen for its versatility as an inherent object storage as well as integration with other AWS services. Further, flexible implementation of rules moving older, infrequently accessed data to S3 Glacier storage can help with storage costs over time. This will tie in with Glue's Data Catalog for my Lakehouse architecture and ad-hoc querying as needed.
    
## Lake Formation, Glue Data Catalog, and Ad-Hoc Data Access
 1. Lake Formation is implemented in this case for fine-grained permission control. It is best practice to exercise data governance via principles of least privilege. However, with Lake Formation enabled on landing, cleaned, and curated S3 buckets and IAMAllowedPrincipals disabled, only specified users are allowed access to data within Lake Formation encapsulated buckets.
 2. Further, Lake Formation integrates with Glue's Data Catalog. This ensures the data lake does not turn into a data swamp. With appropriate metadata capture, both business and technical metadata on all objects posted to the lake storage buckets can be listed to prevent a data swamp.
 3. Lake Formation also allows for ad-hoc querying via Athena's serverless Presto SQL engine. In a larger scale operation, this may be useful for querying data that is not available on a warehouse cluster such as "cold" data, which may be infrequently accessed and stored in S3 Glacier.

## Redshift Serverless
 1. Redshift warehouse will store daily data with STAR schema models loaded into namespace schema.
 2. Redshift warehouse will offer hot data faster querying speeds / lower latencies for analytics and end-user serving (dashboarding).
 3. Appropriate inbound and outbound traffic rules are established for attached VPC to Redshift cluster to prevent unauthorized access to warehouse data.

## Business Intelligence Dashboarding - PowerBI
 1. PowerBI is chosen for its free services and built in connection options to Redshift clusters.
 2. PowerBI also offers scheduled refreshes of data as well as decoupled data source to visual layer depending on license (Pro, Premium, etc.)

# Pipeline Process
## Source Data & Extract
 1. Using Selenium web scraper, I collect data engineering jobs on Glassdoor posted in the last 24 hours (https://www.glassdoor.com/Job/united-states-data-engineer-jobs-SRCH_IL.0,13_IN1_KO14,27.htm?fromAge=1).
 2. Relevant data is scraped from each post. For example, job title is collected for each post as well as all job description text accompanying each job tile. Company information is scraped from each tile and may or may not be present depending on availability of information.
 3. Scraped data is stored as a list of dictionary objects.
 4. List of job details is then converted to JSON object and uploaded to S3 landing bucket.

## Ingestion & Processing
 1. A lambda function is setup to trigger on event object posting of type .json for S3 landing bucket. Lambda function subsequently validates valid values for categorical fields, omits rows with null values for crucial fields as dictated by a threshold. Further cleaning and enhancing such as adding a date field is also performed. These light transformations and cleaning operations are performed using AWS's SDK for pandas library (formerly known as AWS datawrangler).
 2. The lambda function will subsequently write both raw and cleaned datasets to Glue Data Catalog, which is integrated with Lake Formation architecture. This prevents data swamp concerns and ensures both business and technical catalog details are captured for all objects posted into lake storage buckets.
 3. The second lambda function will trigger upon event detection of parquet object posting to the clean bucket. This second lambda function triggers a Glue Spark job to perform heavier data transformations to the daily, cleaned dataset. For example, cleaned data contains salaries as an estimated range string. The Glue Spark job will convert that string to two separate columns with minimum and maximum salaries, both of type float. This is one of the many transformations performed in this step. Data is read from the integrated Glue Data Catalog from the cleaned database. 
 4. Further, the Glue Spark job converts the data to a STAR schema for data modeling purposes. The job will load the dimension tables (scanned into the curated bucket manually using Glue Crawlers) and will be broadcast joined by the Glue Spark job to the transformed dataset and encodings will be selected over their representative categorical fields. Finalized transformed datasets are then moved into the Glue Data Catalog.
 5. All job runs are attached with roles that grant write log access to CloudWatch, thus allowing for debugging upon job fails.

## Loading
 1. Before loading data, SQL schema creation queries are created and run.
 2. A second Glue job to write the finalized DynamicFrame to Redshift would be an appropriate final step of the processing and loading step. This is because the daily posted data will only ever increase incrementally date-wise and new daily data will never count as duplicates in our use case since the web scraping automation is scheduled for scraping every 24 hours.
 3. Rather than a Glue job to write the finalized DynamicFrame to Redshift warehouse, I chose to experiment with Redshift's new preview feature - automated COPY jobs for new objects posted to a specified bucket. In doing so, my warehouse performed COPY jobs on the daily posted curated datasets from the curated bucket.

## Serving
 1. PowerBI is used to connect with the Redshift warehouse. Because data is refreshed daily to the warehouse, PowerBI can be refreshed on the dashboard level. With a Pro license or a Premium License, PowerBI workspaces can be accessed and dataflows may be created alongside scheduled refresh intervals to automatically refresh the dashboard.
 2. End product includes summarized data on companies posting jobs, job requirements and salaries over time, as well as an interactive filterable list of job postings based on skillsets described in job descriptions.
 3. The data model loaded into warehouse and reflected in the PowerBI model is modeled in a STAR schema format. This allows for historical data loading with dimensional slicing and PowerBI's VertiPaq engine to efficiently compress the fact table for low latency performances.
    ![image](https://github.com/kcao22/glassdoor_aws_pipeline/assets/76795831/359721ab-33a4-4608-8d38-946d1e388569)



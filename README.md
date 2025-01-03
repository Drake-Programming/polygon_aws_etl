# Stock Data ETL Pipeline with AWS and Polygon

An end-to-end ETL (Extract, Transform, Load) pipeline for ingesting and processing stock data from the Polygon API, leveraging AWS services for scalability, reliability, and performance. This project demonstrates expertise in cloud-based data engineering, including data extraction, transformation, storage, and monitoring.

## Overview

This pipeline extracts stock data (e.g., timestamp, open, high, low, close, volume, vwap, ticker) from the Polygon API, processes it for analytics, and stores the results in an AWS S3 bucket. 
The architecture is designed to handle batch updates while maintaining cost efficiency and data integrity.

## Features

### Data Pipeline

- **Data Extraction**: Connects to the Polygon API to fetch stock data in batch.
- **Data Transformation**: Cleans, validates, and enriches the raw stock data into an analytics-ready format.
- **Data Loading**: Stores transformed data into an AWS S3 bucket, partitioned by date for efficient querying.

### AWS Integration

- **S3 Data Lake**: Utilized for scalable and cost-effective storage of raw and processed data.

### Scalability and Optimization

- **Partitioned Storage**: Organizes data in `year/month/day` partitions to optimize query performance.

## Technologies Used

- **Languages**: Python (ETL orchestration and transformation logic)
- **API**: Polygon, boto3, moto
- **Cloud Services**: AWS S3, Lambda, Glue, and CloudWatch
- **Data Processing**: Pandas for transformations

## Prerequisites

- Python 3.x installed
- AWS CLI configured
- Access to the Polygon API (API key required)

## Setup and Usage

1. Clone the repository:
- git clone https://github.com/Drake-Programming/polygon_aws_etl.git
- cd polygon_aws_etl

2. Install dependencies:
- pip install -r requirements.txt
  
3. Set up environment variables:
- Add your Polygon API key to the .env file.
- Configure AWS credentials using the AWS CLI.
  
4. Run the pipeline:
- python run.py

## Future Enhancements
- Integration with Analytics Platforms: Enable direct querying via AWS Athena or integration with visualization tools like QuickSight.
- Error Handling and Retry Mechanism: Implement advanced fault-tolerance mechanisms for robust data ingestion.
- CI/CD Pipeline: Automate deployments and updates using AWS CodePipeline or similar tools.

## Contact
For questions or feedback, please contact Robert Wallace.

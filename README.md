# Udacity STEDI Human Balance Analytics

## Project Instructions

Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

![A flowchart displaying the workflow.](/assets/images/flowchart.jpeg "A flowchart displaying the workflow.")

<p align="center">A flowchart displaying the workflow.</p>

![A diagram displaying the relationship between entities.](/assets/images/relation.jpeg "A diagram displaying the relationship between entities.")

<p align="center">A diagram displaying the relationship between entities.</p>

## Structure

- assets
  - [data](/assets/data/): Dataset for Landing zone
  - [images](/assets/images/): Query results, etc.
- scripts
  - [landing](/scripts/landing/): SQL DDL scripts for Landing zone
  - [trusted](/scripts/trusted/): Glue Job Python scripts for Trusted zone
  - [curated](/scripts/curated/): Glue Job Python scripts for Curated zone

## Results

- Landing
  - Customer: 956
  - Accelerometer: 81273
  - Step Trainer: 28680
- Trusted
  - Customer: 482
  - Accelerometer: 40981
  - Step Trainer: 14460
- Curated
  - Customer: 482
  - Machine Learning: 43681

### Athena query results

![Customer Landing](/assets/images/customer_landing.png "Customer Landing")

<p align="center">Customer Landing</p>

![Accelerometer Landing](/assets/images/accelerometer_landing.png "Accelerometer Landing")

<p align="center">Accelerometer Landing</p>

![Customer Trusted](/assets/images/customer_trusted.png "Customer Trusted")

<p align="center">Customer Trusted</p>

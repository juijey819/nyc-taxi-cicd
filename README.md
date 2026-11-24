
# NYC Taxi Data Ingestion CI/CD

## Project Overview

An automated data ingestion pipeline using Python,
GitHub Actions, and Azure Blob Storage.

## Data Sources

- 2023 Yellow Taxi Trip Data
- NYC Taxi Zones

## Architecture

NYC Open Data API
    -> Python Ingestion
    -> Validation
    -> Transformation
    -> GitHub Actions
    -> Azure Blob Storage

## Features

- Batch API ingestion
- Data validation
- Data transformation
- Automated unit testing
- Git branch and pull request
- Azure Blob Storage integration
- Six-hour scheduled execution

## Storage Containers

- raw
- processed
- reference
- metadata

## Scheduler

Every 6 hours using GitHub Actions.

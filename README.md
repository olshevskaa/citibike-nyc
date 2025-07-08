# Citi Bike NYC

## Project Overview
This is an end-to-end project for Citi Bike bycicle sharing system ride time analysis and prediction, starting with an Apache Airflow pipeline to collect the data, followed by full analysis, hypothesis testing, and predictive modeling using Random Forest.

## Repository Structure
```
│
├── airflow/
│   ├── config/
│   │   ├── airflow.cfg # Airflow base configuration
│   └── dags/
│       └── citibike_pipeline.py # Airflow DAG for ETL pipeline
│
├── notebooks/
│   └── citibike.ipynb # Notebook for analysis/visualization
│
├── docker-compose.yaml # Docker configuration for Airflow
├── Dockerfile # Custom Docker image definition
├── requirements.txt # Python dependencies for Airflow DAG
└── README.md # This file
```

## Notebooks
- **citibike.ipynb** - main notebook, containing data cleaning, processing, analysis, hypothesis testing and modeling.

## DAGs
- **citibike_pipeline.py** - main DAG, reponsible for extracting Citi Bike data, processing it and loading to BigQuery warehouse.

## Technologies Used

- **Apache Airflow** - for pipeline orchestration
- **BigQuery** - data warehouse
- **Docker** - for containerized development
- **Python** - scripting and data processing
- **Jupyter Notebook** - analysis and visualization
- **Pandas / Matplotlib / Seaborn** - for data wrangling and plotting
- **Scipy / Statsmodels / Sklearn** - for hypothesis testing and prediction model building

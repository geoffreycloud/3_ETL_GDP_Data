# Country GDP ETL Pipeline
_Technologies Used: Python,Beautiful Soup, requests, pandas, SQLite_  

## Summary
This project implements an ETL pipeline to collect, clean, and store country GDP data. The pipeline scrapes publicly available GDP data from Wikipedia, processes and transform the values, and loads the cleaned dataset into both a CSV file and a relational database for further analysis.
The primary focus of this project is to practice web scraping techniques and ETL pipeline design using Python, as part of a data engineering course.

## Data Source  
[Archived Wikipedia page: List of countries by GDP](https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29)  

## Project Structure

- [etl_project_gdp.py](https://github.com/geoffreycloud/3_ETL_GDP_Data/blob/main/etl_project_gdp.py) – Contains all extract, transform, load, and logging logic
- [GDP_USD_Billions.csv](https://github.com/geoffreycloud/3_ETL_GDP_Data/blob/main/GDP_USD_Billions.csv) – Stores the final cleaned GDP dataset
- [Country_GDP.db](https://github.com/geoffreycloud/3_ETL_GDP_Data/blob/main/Country_GDP.db) – Stores transformed data for querying
- [etl_project_log.txt](https://github.com/geoffreycloud/3_ETL_GDP_Data/blob/main/etl_project_log.txt) – Records execution progress and pipeline stages

## Purpose  
This project was completed as part of a data engineering course to build hands-on experience with web scraping, data cleaning, and ETL workflows. It demonstrates how raw web data can be transformed into structured datasets suitable for analysis and storage.

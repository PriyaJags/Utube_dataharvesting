# Project Title: YouTube Data Harvesting and Warehousing

## Overview
This project focuses on harvesting data from YouTube using Python scripting, storing and managing it in databases such as MongoDB and SQL, and providing a graphical user interface through Streamlit. The aim is to enable users to collect, store, and analyze YouTube data efficiently.

## Key Skills Developed
- Python scripting for data collection from YouTube
- MongoDB for NoSQL database management
- SQL for relational database management
- Streamlit for creating an interactive user interface
- API integration for accessing YouTube data
- Data management techniques using MongoDB (Atlas) and SQL
  
## Domain
The project operates within the domain of Social Media, specifically targeting YouTube data.

## Approach: 
- Set up a Streamlit app: Streamlit is a great choice for building user friendly interface quickly and easily. You can use Streamlit to create a simple UI where users can enter a YouTube channel ID, retrieve all channel details, and select channels to migrate to the data warehouse.
- Connect to the YouTube API: You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.
- Store data in a MongoDB data lake: Once you retrieve the data from the YouTube API, you can store it in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.
- Migrate data to a SQL data warehouse: After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL or PostgreSQL for this.
- Query the SQL data warehouse: You can use SQL queries in the SQL data warehouse and retrieve data for specific channels based on user input.
- PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing and analysis.
- Display data in the Streamlit app: Finally, you can display the retrieved data in the Streamlit app. 

## LIBRARIES USED: 
- googleapiclient.discovery
- streamlit
- psycopg2
- pymongo
- pandas   give in this format for expense tracker 

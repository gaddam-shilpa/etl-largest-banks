# Largest Banks ETL Project 
**Acquiring and processing information on worlds largest banks**

## 📖 Project Overview
This project demonstrates the design and implementation of an ETL (Extract, Transform, Load) pipeline using Python.  
The goal was to work with real-world data and perform the operations of **Extraction, Transformation, and Loading (ETL)** in a professional workflow.

---

## 🌍 Project Scenario
A multi-national firm has hired me as a Data Engineer.  
My job was to access and process data as per requirements.

The task was to compile a list of the **top 10 largest banks in the world ranked by market capitalization (in billion USD)**.  
Further, the data had to be **transformed and stored in USD, GBP, EUR, and INR**, based on the exchange rate information provided in a CSV file.  

The processed information had to be saved:  
- Locally as a **CSV file**  
- In a **database table** (SQLite)  

Finally, managers from different countries could query the database table to extract the list and view the market capitalization in their own currency.

---

## ✅ Tasks Completed
1. Extracted tabular information from a Wikipedia URL into a DataFrame.  
2. Transformed the DataFrame by adding columns for Market Capitalization in GBP, EUR, and INR (rounded to 2 decimal places) using the exchange rate CSV file.  
3. Loaded the transformed DataFrame into a CSV file.  
4. Loaded the transformed DataFrame into a SQLite database table.  
5. Ran SQL queries on the database table:  
   - Extracted information for the **London office**: Name and MC_GBP_Billion  
   - Extracted information for the **Berlin office**: Name and MC_EUR_Billion  
   - Extracted information for the **New Delhi office**: Name and MC_INR_Billion  
6. Logged the progress of the ETL pipeline execution in a log file.  

---

## 🛠️ Tools & Libraries
- **pandas** → data storage and manipulation  
- **BeautifulSoup (bs4)** → parsing HTML  
- **requests** → retrieving the webpage content  
- **sqlite3** → creating and querying the database  

---

## 🔎 Approach
- Accessed the required webpage using `requests.get().text`.  
- Parsed the HTML using **BeautifulSoup** to extract the bank table.  
- Transformed and enriched the data with multiple currency columns.  
- Saved the processed data into **CSV** and **SQLite database** formats.  
- Logged each stage of execution in a **log file**.  

---

## 📊 Output
With this project, I successfully performed ETL operations on real-world data and made the processed information available in multiple formats.  

Key outcomes:  
- Data scraped from a real-world source (Wikipedia).  
- Transformed into multiple currencies.  
- Available for analysis in **CSV** and **SQLite database**.  
- Queries to support country-specific reporting.  

---

## 🙌 Acknowledgement
This project was inspired by and completed as part of the **IBM Data Engineering Professional Certificate**.  

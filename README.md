## Overview

This project automates the collection of individual broker records from FINRA (Financial Industry Regulatory Authority) using their public search API. It processes MPID (Market Participant Identifier) records from a Google Sheet, fetches related individual data, and appends the results to a master spreadsheet with deduplication.

## Features

- **Batch Processing** – Process MPIDs in configurable batches with rate limiting
- **Google Sheets Integration** – Read source MPIDs and write results directly to Google Sheets
- **Automatic Deduplication** – Prevents duplicate records based on `ind_source_id`
- **Pagination Handling** – Automatically fetches all pages of results for each MPID
- **Workbook Size Management** – Monitors Google Sheets 10M cell limit and rolls over to new spreadsheets when needed
- **Sheet Compaction** – Resizes worksheets to free allocated cells and stay within limits
- **Configurable Processing** – Adjust batch sizes, sleep intervals, and temp sheet clearing frequency

## API Source

Data is sourced from FINRA's public BrokerCheck API:
https://api.brokercheck.finra.org/search/individual

text

## Prerequisites

- Python 3.7+
- Google Cloud Platform account with Sheets API enabled
- Service account credentials JSON file

## Installation

```bash
git clone https://github.com/yourusername/finra-brokercheck-scraper.git
cd finra-brokercheck-scraper
pip install -r requirements.txt
Google Sheets Setup
Create a Google Cloud Project and enable the Google Sheets API

Create a Service Account and download the credentials JSON

Share your target spreadsheets with the service account email

Create a spreadsheet with these worksheets:

Worksheet	Purpose
FINRA - MPID Records	Contains MPID list and processing flags
BrokerCheck - Individual	Destination for scraped records
BrokerCheck - Individuals - TEMP	Temporary staging sheet
MPID Sheet Structure
Column	Description
MPID	Market Participant Identifier to process
(Flag column)	Set to TRUE after processing (any column containing "flag")
Configuration
Edit the following variables in finra_scraper_safe.py:

python
CREDENTIALS_FILE = "credentials.json"      # Your service account key
SPREADSHEET_NAME = "Your Spreadsheet Name"
MAX_MPIDS_PER_RUN = 50                     # Batch size
TEMP_CLEAR_EVERY_N_MPIDS = 1               # Temp sheet clearing frequency
Usage
bash
# Process default number of MPIDs (configured in script)
python finra_scraper_safe.py

# Process specific number of MPIDs
python finra_scraper_safe.py --max-mpids 25

# Clear temp sheet every 5 MPIDs
python finra_scraper_safe.py --temp-clear-every 5
Utility Scripts
CSV Cleaner (append_csv.py)
Cleans corrupted CSV files by removing malformed JSON/dict columns that appear at row boundaries.

Excel Appender (append_excel.py)
Command-line utility to append one Excel file to another:

bash
python append_excel.py data1.xlsx data2.xlsx combined.xlsx
Google Sheets Copier (copy_brokercheck.py)
Copies all data rows from one Google Sheet to another while preserving headers, using batch writes for efficiency.

Data Fields Collected
Field	Description
ind_source_id	Unique individual identifier
ind_firstname	First name
ind_middlename	Middle name
ind_lastname	Last name
ind_other_names	Other known names
ind_bc_scope	BrokerCheck scope
ind_ia_scope	Investment Advisor scope
ind_bc_disclosure_fl	Disclosure flag
ind_approved_finra_registration_count	FINRA registration count
ind_employments_count	Number of employments
ind_industry_cal_date	Industry calendar date
ind_current_employments	Current employment details
highlight	Search highlight data
MPID	Source MPID
Rate Limiting & API Courtesy
The script includes configurable delays to respect FINRA's API:

page_sleep_seconds = 0.25 – Delay between paginated requests

mpid_sleep_seconds = 1.0 – Delay between MPID processing

Error Handling & Recovery
Failed MPIDs are flagged as processed (skipped on next run)

Master sheet rollover when approaching 10M cell limit

Temp sheet clearing prevents workbook bloat

Backup creation before CSV cleaning operations

Limitations
Google Sheets has a 10 million cell limit per workbook

FINRA API may have undocumented rate limits

Large result sets may require multiple passes

Security Best Practices
Before publishing this repository:

Delete or revoke the credentials in credentials.json from Google Cloud Console

Create a new service account and download fresh credentials

Add credentials.json to .gitignore

Remove any hardcoded spreadsheet IDs from copy_brokercheck.py

Use environment variables for sensitive values:

python
import os

# Instead of hardcoding:
CREDENTIALS_FILE = os.getenv("GCP_CREDENTIALS_FILE", "credentials.json")
SPREADSHEET_NAME = os.getenv("FINRA_SPREADSHEET_NAME")
License
MIT

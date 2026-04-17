# FINRA
FINRA BrokerCheck Scraper - Python tool that extracts financial broker data from FINRA's API to Google Sheets. Fetches employment history, disclosures &amp; registrations using MPIDs. Auto-deduplicates, handles pagination, marks progress. Configurable batch processing.
Here's a comprehensive GitHub-ready description for your FINRA BrokerCheck scraper:

---

# FINRA BrokerCheck Scraper

Automated data collection tool that extracts financial advisor and brokerage firm professional backgrounds from FINRA's BrokerCheck API and stores them directly in Google Sheets.

## Overview

This scraper systematically retrieves detailed employment history, disclosure events, registration status, and professional qualifications for financial industry professionals using their MPIDs (Market Participant Identifiers). It's designed for compliance officers, researchers, and data analysts who need to build and maintain a structured database of broker information.

## Features

- **Automated Data Collection**: Fetches comprehensive broker profiles from FINRA's official BrokerCheck API
- **Google Sheets Integration**: Directly writes structured data to three Google Sheets worksheets for easy access and analysis
- **Deduplication Logic**: Automatically prevents duplicate records by tracking unique broker IDs
- **Resumable Processing**: Marks processed MPIDs with a flag, allowing for incremental runs without rework
- **Rate-Limit Friendly**: Implements configurable delays between API requests to avoid overwhelming the service
- **Pagination Handling**: Automatically retrieves all pages of results (up to 100 records per request)
- **Fault Tolerance**: Continues processing remaining MPIDs even if individual requests fail
- **Audit Trail**: Maintains both raw (TEMP) and deduplicated (MASTER) data stores

## Data Fields Collected

| Field | Description |
|-------|-------------|
| `ind_source_id` | Unique broker identifier |
| `ind_firstname` | First name |
| `ind_middlename` | Middle name |
| `ind_lastname` | Last name |
| `ind_other_names` | Alternate names/aliases |
| `ind_bc_scope` | BrokerCheck scope (Broker/Investment Adviser) |
| `ind_ia_scope` | Investment adviser scope |
| `ind_bc_disclosure_fl` | Disclosure flag (any reported incidents) |
| `ind_approved_finra_registration_count` | Number of FINRA registrations |
| `ind_employments_count` | Number of employment records |
| `ind_industry_cal_date` | Industry experience date |
| `ind_current_employments` | Current employer information |
| `highlight` | Search result highlights |
| `MPID` | Source Market Participant ID |

## Requirements

- Python 3.7+
- Google Cloud Platform account with Sheets API enabled
- Service account credentials (JSON file)
- Google Sheets with three pre-configured worksheets

### Python Dependencies

```
pandas
requests
gspread
gspread-dataframe
oauth2client
```

## Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/finra-brokercheck-scraper.git
cd finra-brokercheck-scraper
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Set up Google Sheets API**
   - Create a project in [Google Cloud Console](https://console.cloud.google.com/)
   - Enable Google Sheets API and Google Drive API
   - Create a service account and download the JSON credentials file
   - Share your target spreadsheet with the service account email

4. **Prepare your spreadsheet**
   - Create three worksheets:
     - `FINRA - MPID Records` (contains MPIDs to scrape + a flag column)
     - `BrokerCheck - Individuals - TEMP` (stores raw scraped data)
     - `BrokerCheck - Individual` (master database of unique records)

5. **Configure the script**
   - Place your `credentials.json` in the project directory
   - Update `SPREADSHEET_NAME` in the main block
   - (Optional) Configure proxy settings if needed

## Usage

### Basic Usage

```python
from finra_scraper import FINRABrokerCheckScraper

scraper = FINRABrokerCheckScraper(
    google_sheets_creds_file="credentials.json",
    spreadsheet_name="Your Spreadsheet Name",
    proxy=None  # Optional: {"http": "http://proxy:port", "https": "https://proxy:port"}
)

# Process up to 10 MPIDs in this run
scraper.run(max_mpids=10)
```

### Command Line

```bash
python finra_scraper.py
```

Modify the `MAX_MPIDS_PER_RUN` variable in the main block to control batch size.

## MPID Worksheet Structure

Your MPID worksheet should have at minimum:

| Column A (MPID) | Column F (Flag) |
|----------------|-----------------|
| ABC123 | FALSE |
| XYZ789 | FALSE |
| DEF456 | TRUE |

- **MPID Column**: Any column with header "MPID"
- **Flag Column**: Any column containing "flag" in the header (case-insensitive)
- Rows with `FALSE` flag are processed; `TRUE` indicates completion

## How It Works

1. **Preflight Check**: Validates credentials, worksheets, and required columns
2. **MPID Discovery**: Identifies unprocessed MPIDs (flag = FALSE)
3. **API Request**: Fetches broker data from FINRA's search endpoint
4. **Pagination**: Automatically retrieves all result pages
5. **Data Transformation**: Maps API response to structured fields
6. **Deduplication**: Checks master sheet for existing records by `ind_source_id`
7. **Storage**: 
   - Appends all records to TEMP sheet (audit trail)
   - Appends only new records to MASTER sheet
8. **Flag Update**: Marks MPID as processed in the source sheet
9. **Repeat**: Processes next MPID after configurable delay

## API Endpoint

The scraper uses FINRA's public BrokerCheck API:
```
https://api.brokercheck.finra.org/search/individual?query={MPID}&includePrevious=true&nrows=100&start={offset}&wt=json
```

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `page_sleep_seconds` | 0.25 | Delay between paginated requests |
| `mpid_sleep_seconds` | 1.0 | Delay between different MPID requests |
| `MAX_MPIDS_PER_RUN` | 10 | Maximum MPIDs to process in one execution |
| `nrows` | 100 | Results per API page (API maximum) |

## Error Handling

- **Network failures**: Logs error and continues to next MPID
- **Missing columns**: Preflight check prevents execution
- **Duplicate records**: Filtered out at master sheet insertion
- **JSON decode errors**: Skips malformed responses
- **Rate limiting**: Configurable delays prevent throttling

## Limitations

- FINRA API may have undocumented rate limits
- Maximum 100 records per API request
- Requires manual spreadsheet setup before first run
- No built-in scheduling (use cron/Task Scheduler for automation)

## Use Cases

- **Compliance Monitoring**: Track broker employment history and disclosure events
- **Research**: Build datasets for financial industry analysis
- **Background Checks**: Automate preliminary due diligence
- **Data Integration**: Feed broker data into CRM or compliance systems
- **Audit Trails**: Maintain historical records of broker status changes

## Contributing

Contributions welcome! Areas for improvement:
- Add support for firm-level data collection
- Implement incremental updates (detect changed records)
- Add email notifications for new disclosures
- Create dashboard templates for data visualization
- Add command-line argument parsing



- **1.0.0** - Initial release
  - Individual broker data collection
  - Google Sheets integration
  - Automatic pagination and deduplication

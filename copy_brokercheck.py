import logging
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# CONFIGURATION
# ------------------------------------------------------------------ #
CREDENTIALS_FILE      = "credentials.json"
SHEET_NAME            = "BrokerCheck - Individual"
SOURCE_SPREADSHEET_ID = "1lrj7BZW3EVQRhbsb-Oz-14gbrwfhHDVP7KJ0Gja0C0A"  # Copy of N8N - Project All Records
DEST_SPREADSHEET_ID   = "1jLxYqioeqEWEPDYa7w6lATjVObjUBflVxlpPBM1fR0A"  # N8N - Project All Records
# ------------------------------------------------------------------ #

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_service():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def read_all_rows(service, spreadsheet_id: str, sheet_name: str) -> list:
    """Read every row including header."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1:ZZ",
        ).execute()
        rows = result.get("values", [])
        logger.info("Read %s total row(s) from source (including header)", len(rows))
        return rows
    except HttpError as exc:
        logger.error("Failed to read source sheet: %s", exc)
        raise


def clear_data_rows(service, spreadsheet_id: str, sheet_name: str) -> None:
    """Clear everything below the header row in the destination."""
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A2:ZZ",
        ).execute()
        logger.info("Cleared existing data rows in destination (header kept)")
    except HttpError as exc:
        logger.error("Failed to clear destination sheet: %s", exc)
        raise


def write_data_rows(service, spreadsheet_id: str, sheet_name: str, rows: list, batch_size: int = 500) -> None:
    """Write rows in batches to avoid request timeouts."""
    if not rows:
        logger.warning("No data rows to write — source has no data below the header.")
        return

    total_batches = (len(rows) + batch_size - 1) // batch_size
    logger.info("Writing %s row(s) in %s batch(es) of %s", len(rows), total_batches, batch_size)

    for i in range(0, len(rows), batch_size):
        batch     = rows[i: i + batch_size]
        start_row = i + 2           # +2: row 1 is header, rows are 1-based
        batch_num = (i // batch_size) + 1

        try:
            result = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A{start_row}",
                valueInputOption="USER_ENTERED",
                body={"values": batch},
            ).execute()
            logger.info(
                "Batch %s/%s — wrote %s row(s) → %s",
                batch_num, total_batches,
                result.get("updatedRows"),
                result.get("updatedRange"),
            )
        except HttpError as exc:
            logger.error("Failed on batch %s/%s: %s", batch_num, total_batches, exc)
            raise


def main():
    logger.info("Connecting to Google Sheets API...")
    service = get_service()

    # Step 1: Read all rows from source
    logger.info("Reading from 'Copy of N8N - Project All Records' → '%s'", SHEET_NAME)
    all_rows = read_all_rows(service, SOURCE_SPREADSHEET_ID, SHEET_NAME)

    if len(all_rows) <= 1:
        logger.warning("Source sheet has no data rows below the header. Nothing to copy.")
        return

    data_rows = all_rows[1:]  # strip header
    logger.info("%s data row(s) to copy", len(data_rows))

    # Step 2: Clear destination data rows (keep header)
    logger.info("Clearing 'N8N - Project All Records' → '%s'", SHEET_NAME)
    clear_data_rows(service, DEST_SPREADSHEET_ID, SHEET_NAME)

    # Step 3: Write source data into destination
    logger.info("Writing data into 'N8N - Project All Records' → '%s'", SHEET_NAME)
    write_data_rows(service, DEST_SPREADSHEET_ID, SHEET_NAME, data_rows)

    logger.info("Done! Data copied successfully. Headers untouched in both sheets.")


if __name__ == "__main__":
    main()

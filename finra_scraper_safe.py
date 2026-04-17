import logging
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class FINRABrokerCheckScraper:
    # Keep a stable column order for append_rows() calls.
    RECORD_COLUMNS = [
        "ind_source_id",
        "ind_firstname",
        "ind_middlename",
        "ind_lastname",
        "ind_other_names",
        "ind_bc_scope",
        "ind_ia_scope",
        "ind_bc_disclosure_fl",
        "ind_approved_finra_registration_count",
        "ind_employments_count",
        "ind_industry_cal_date",
        "ind_current_employments",
        "highlight",
        "MPID",
    ]

    @staticmethod
    def _coerce_cell_value(value) -> str:
        """Google Sheets cells can't receive Python lists/dicts; coerce to scalar."""
        if value is None:
            return ""
        if isinstance(value, (list, dict)):
            try:
                return json.dumps(value, ensure_ascii=False)
            except Exception:
                return str(value)
        # Keep primitives as strings/numbers; API expects cell scalar values.
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        return str(value)

    def __init__(
        self,
        google_sheets_creds_file: str,
        spreadsheet_name: str,
        proxy: Optional[Dict] = None,
    ):
        self.creds_file = Path(google_sheets_creds_file)
        self.spreadsheet_name = spreadsheet_name
        self.proxy = proxy

        self.mpid_sheet_name = "FINRA - MPID Records"
        self.temp_sheet_name = "BrokerCheck - Individuals - TEMP"
        self.individual_sheet_name = "BrokerCheck - Individual"

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        self.client = None
        self.spreadsheet = None

        # Tunables to speed up runs while still being gentle on APIs.
        self.page_sleep_seconds = 0.25
        self.mpid_sleep_seconds = 1.0

        # Cached sheet metadata/state to avoid re-reading large sheets per MPID.
        self.mpid_col_index: Optional[int] = None
        self.flag_col_index: Optional[int] = None  # 0-based, for MPID sheet
        # Note: avoid set[str] typing for older Python versions.
        self.master_existing_ids: Optional[set] = None

    def preflight_check(self) -> bool:
        """Validate config, credentials, worksheet names, and required columns."""
        if not self.creds_file.exists():
            logger.error("credentials file not found: %s", self.creds_file)
            return False

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(str(self.creds_file), scope)
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open(self.spreadsheet_name)
            logger.info("Connected to spreadsheet: %s", self.spreadsheet_name)
        except Exception as exc:
            logger.error("Unable to connect to Google Sheets: %s", exc)
            return False

        required_tabs = [
            self.mpid_sheet_name,
            self.temp_sheet_name,
            self.individual_sheet_name,
        ]
        existing_tabs = {ws.title for ws in self.spreadsheet.worksheets()}
        missing_tabs = [tab for tab in required_tabs if tab not in existing_tabs]
        if missing_tabs:
            logger.error("Missing worksheet tabs: %s", ", ".join(missing_tabs))
            return False

        mpid_ws = self.spreadsheet.worksheet(self.mpid_sheet_name)
        headers = mpid_ws.row_values(1)
        if not headers:
            logger.error("MPID worksheet has no header row")
            return False

        mpid_col_index = self._find_mpid_col(headers)
        flag_col_index, flag_col_name = self._find_flag_col(headers)

        if mpid_col_index is None:
            logger.error("MPID column not found in '%s'", self.mpid_sheet_name)
            return False
        if flag_col_index is None:
            logger.error("Flag column not found in '%s'", self.mpid_sheet_name)
            return False

        logger.info("Preflight passed. MPID column: %s, Flag column: %s", headers[mpid_col_index], flag_col_name)

        # Cache indexes for speed.
        self.mpid_col_index = mpid_col_index
        self.flag_col_index = flag_col_index

        # Cache existing master IDs once per run.
        try:
            master_ws = self.spreadsheet.worksheet(self.individual_sheet_name)
            master_df = get_as_dataframe(master_ws, evaluate_formulas=False)
            if "ind_source_id" in master_df.columns:
                self.master_existing_ids = set(master_df["ind_source_id"].dropna().astype(str))
            else:
                self.master_existing_ids = set()
        except Exception as exc:
            # If this fails, we still can run, but dedupe becomes less effective.
            logger.warning("Could not load master existing IDs; dedupe may be less effective: %s", exc)
            self.master_existing_ids = set()
        return True

    @staticmethod
    def _find_mpid_col(headers: List[str]) -> Optional[int]:
        for idx, header in enumerate(headers):
            if header.strip().upper() == "MPID":
                return idx
        return None

    @staticmethod
    def _find_flag_col(headers: List[str]) -> Tuple[Optional[int], Optional[str]]:
        for idx, header in enumerate(headers):
            if "flag" in header.lower():
                return idx, header
        if len(headers) > 5:
            return 5, headers[5] if headers[5] else "Column F"
        return None, None

    def get_unprocessed_mpids(self, limit: int = 1) -> List[Dict]:
        try:
            worksheet = self.spreadsheet.worksheet(self.mpid_sheet_name)
            all_values = worksheet.get_all_values()

            if not all_values:
                logger.warning("Sheet '%s' is empty", self.mpid_sheet_name)
                return []

            headers = all_values[0]
            mpid_col_index = self.mpid_col_index if self.mpid_col_index is not None else self._find_mpid_col(headers)
            flag_col_index, _flag_col_name = (
                (self.flag_col_index, None) if self.flag_col_index is not None else self._find_flag_col(headers)
            )

            if mpid_col_index is None or flag_col_index is None:
                logger.error("Required columns missing. MPID: %s, Flag: %s", mpid_col_index, flag_col_index)
                return []

            logger.info("Using MPID column at index %s and flag column at index %s", mpid_col_index, flag_col_index)

            unprocessed = []
            for row_idx, row in enumerate(all_values[1:], start=2):
                mpid = row[mpid_col_index].strip() if len(row) > mpid_col_index and row[mpid_col_index] else None
                raw_flag = row[flag_col_index].strip().upper() if len(row) > flag_col_index and row[flag_col_index] else "FALSE"

                if mpid and raw_flag == "FALSE":
                    unprocessed.append({"MPID": mpid, "row": row_idx, "data": row})
                if len(unprocessed) >= limit:
                    break

            logger.info("Found %s unprocessed MPID(s)", len(unprocessed))
            return unprocessed
        except Exception as exc:
            logger.error("Error reading MPID sheet: %s", exc)
            return []

    def fetch_individual_data(self, mpid: str, start: int = 0, nrows: int = 100) -> Optional[Dict]:
        url = (
            "https://api.brokercheck.finra.org/search/individual"
            f"?query={mpid}&includePrevious=true&nrows={nrows}&start={start}&wt=json"
        )
        try:
            logger.info("Fetching data for MPID %s (start=%s)", mpid, start)
            response = requests.get(url, headers=self.headers, proxies=self.proxy, timeout=30)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                logger.warning("Unexpected JSON type for MPID %s: %s", mpid, type(payload))
                return None
            return payload
        except requests.exceptions.RequestException as exc:
            logger.error("Request failed for MPID %s (start=%s): %s", mpid, start, exc)
            return None
        except ValueError as exc:
            logger.error("JSON decode failed for MPID %s (start=%s): %s", mpid, start, exc)
            return None

    def fetch_all_pages(self, mpid: str, total_hits: int) -> List[Dict]:
        all_hits: List[Dict] = []
        nrows = 100
        total_pages = (total_hits + nrows - 1) // nrows

        for page in range(total_pages):
            start = page * nrows
            data = self.fetch_individual_data(mpid, start=start, nrows=nrows)
            if not data:
                logger.warning("Skipping page %s/%s due to empty response", page + 1, total_pages)
                continue

            hits_container = data.get("hits") or {}
            if not isinstance(hits_container, dict):
                logger.warning("Unexpected 'hits' container type on page %s/%s: %s", page + 1, total_pages, type(hits_container))
                continue

            hits = hits_container.get("hits", [])
            if not isinstance(hits, list):
                logger.warning("Unexpected 'hits' shape on page %s/%s", page + 1, total_pages)
                continue

            all_hits.extend(hits)
            logger.info("Page %s/%s returned %s record(s)", page + 1, total_pages, len(hits))

            if page < total_pages - 1:
                time.sleep(self.page_sleep_seconds)

        return all_hits

    def fetch_all_pages_from(self, mpid: str, total_hits: int, start_page: int) -> List[Dict]:
        """Fetch paginated results starting at `start_page` (0-based)."""
        if start_page < 0:
            start_page = 0
        nrows = 100
        total_pages = (total_hits + nrows - 1) // nrows
        if start_page >= total_pages:
            return []

        all_hits: List[Dict] = []
        for page in range(start_page, total_pages):
            start = page * nrows
            data = self.fetch_individual_data(mpid, start=start, nrows=nrows)
            if not data:
                logger.warning("Skipping page %s/%s due to empty response", page + 1, total_pages)
                continue

            hits_container = data.get("hits") or {}
            if not isinstance(hits_container, dict):
                logger.warning(
                    "Unexpected 'hits' container type on page %s/%s: %s",
                    page + 1,
                    total_pages,
                    type(hits_container),
                )
                continue

            hits = hits_container.get("hits", [])
            if not isinstance(hits, list):
                logger.warning("Unexpected 'hits' shape on page %s/%s", page + 1, total_pages)
                continue

            all_hits.extend(hits)
            logger.info("Page %s/%s returned %s record(s)", page + 1, total_pages, len(hits))

            if page < total_pages - 1:
                time.sleep(self.page_sleep_seconds)

        return all_hits

    @staticmethod
    def transform_record(record: Dict, mpid: str) -> Dict:
        source = record.get("_source", {})
        highlight = record.get("highlight", {})
        return {
            "ind_source_id": source.get("ind_source_id"),
            "ind_firstname": source.get("ind_firstname"),
            "ind_middlename": source.get("ind_middlename"),
            "ind_lastname": source.get("ind_lastname"),
            "ind_other_names": source.get("ind_other_names"),
            "ind_bc_scope": source.get("ind_bc_scope"),
            "ind_ia_scope": source.get("ind_ia_scope"),
            "ind_bc_disclosure_fl": source.get("ind_bc_disclosure_fl"),
            "ind_approved_finra_registration_count": source.get("ind_approved_finra_registration_count"),
            "ind_employments_count": source.get("ind_employments_count"),
            "ind_industry_cal_date": source.get("ind_industry_cal_date") or source.get("ind_industry_days"),
            "ind_current_employments": source.get("ind_current_employments"),
            "highlight": str(highlight) if highlight else None,
            "MPID": mpid,
        }

    def update_temp_sheet(self, records: List[Dict]) -> None:
        if not records:
            return
        worksheet = None
        try:
            worksheet = self.spreadsheet.worksheet(self.temp_sheet_name)
            rows = [
                [self._coerce_cell_value(rec.get(col)) for col in self.RECORD_COLUMNS]
                for rec in records
            ]
            # append_rows avoids re-reading the full sheet to find next_row.
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")
            logger.info("Appended %s record(s) to temp sheet", len(records))
        except Exception as exc:
            logger.error("Error updating temp sheet (append_rows): %s", exc)
            # Fallback to previous behavior (slower but more compatible).
            try:
                if worksheet is None:
                    worksheet = self.spreadsheet.worksheet(self.temp_sheet_name)
                existing_df = get_as_dataframe(worksheet, evaluate_formulas=False)
                next_row = len(existing_df) + 2
                df = pd.DataFrame(
                    [
                        [self._coerce_cell_value(rec.get(col)) for col in self.RECORD_COLUMNS]
                        for rec in records
                    ],
                    columns=self.RECORD_COLUMNS,
                )
                set_with_dataframe(worksheet, df, row=next_row, col=1, include_column_header=False)
                logger.info("Added %s record(s) to temp sheet (fallback)", len(records))
            except Exception as exc2:
                logger.error("Error updating temp sheet (fallback): %s", exc2)

    def update_master_sheet(self, records: List[Dict]) -> None:
        if not records:
            return
        try:
            worksheet = self.spreadsheet.worksheet(self.individual_sheet_name)
            if self.master_existing_ids is None:
                self.master_existing_ids = set()

            new_records = []
            for row in records:
                rid = row.get("ind_source_id")
                if not rid:
                    continue
                rid_str = str(rid)
                if rid_str in self.master_existing_ids:
                    continue
                self.master_existing_ids.add(rid_str)
                new_records.append(row)

            if not new_records:
                logger.info("No new records to append to master sheet")
                return

            rows = [
                [self._coerce_cell_value(rec.get(col)) for col in self.RECORD_COLUMNS]
                for rec in new_records
            ]
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")
            logger.info("Appended %s new record(s) to master sheet", len(new_records))
        except Exception as exc:
            logger.error("Error updating master sheet: %s", exc)

    def update_mpid_flag(self, mpid: str, row_index: int) -> None:
        try:
            worksheet = self.spreadsheet.worksheet(self.mpid_sheet_name)
            if self.flag_col_index is None:
                headers = worksheet.row_values(1)
                flag_col_index, _ = self._find_flag_col(headers)
                if flag_col_index is None:
                    logger.error("Cannot update flag for MPID %s. Flag column not found.", mpid)
                    return
            else:
                flag_col_index = self.flag_col_index

            worksheet.update_cell(row_index, flag_col_index + 1, "TRUE")
            logger.info("Updated MPID flag for %s at row %s", mpid, row_index)
        except Exception as exc:
            logger.error("Error updating MPID flag for %s: %s", mpid, exc)

    def process_single_mpid(self, mpid_info: Dict) -> None:
        mpid = mpid_info["MPID"]
        row = mpid_info["row"]
        logger.info("Processing MPID %s (row %s)", mpid, row)

        initial_data = self.fetch_individual_data(mpid)
        if not initial_data:
            logger.error("Initial fetch failed for MPID %s", mpid)
            self.update_mpid_flag(mpid, row)
            return

        initial_hits_container = initial_data.get("hits") or {}
        if not isinstance(initial_hits_container, dict):
            logger.warning("Unexpected initial 'hits' container for MPID %s: %s", mpid, type(initial_hits_container))
            initial_hits_container = {}

        total_hits = initial_hits_container.get("total", 0)
        if isinstance(total_hits, dict):
            total_hits = total_hits.get("value", 0)

        logger.info("Total hits for MPID %s: %s", mpid, total_hits)

        if total_hits > 1:
            initial_hits = initial_hits_container.get("hits", [])
            if not isinstance(initial_hits, list):
                initial_hits = []
            all_hits = initial_hits
            all_hits.extend(self.fetch_all_pages_from(mpid, total_hits, start_page=1))
        elif total_hits == 1:
            all_hits = initial_hits_container.get("hits", [])
            if not isinstance(all_hits, list):
                logger.warning("Unexpected single-result hits payload for MPID %s", mpid)
                all_hits = []
        else:
            logger.info("No results for MPID %s", mpid)
            self.update_mpid_flag(mpid, row)
            return

        transformed = [self.transform_record(record, mpid) for record in all_hits]

        unique_records = []
        seen_ids = set()
        for record in transformed:
            record_id = record.get("ind_source_id")
            if record_id and record_id not in seen_ids:
                seen_ids.add(record_id)
                unique_records.append(record)

        logger.info("Transformed %s rows (%s unique IDs)", len(transformed), len(unique_records))
        self.update_temp_sheet(transformed)
        self.update_master_sheet(unique_records)
        self.update_mpid_flag(mpid, row)

    def run(self, max_mpids: int = 1) -> None:
        if not self.preflight_check():
            logger.error("Preflight failed. Exiting.")
            return

        mpid_records = self.get_unprocessed_mpids(limit=max_mpids)
        if not mpid_records:
            logger.info("No unprocessed MPIDs found")
            return

        for record in mpid_records:
            try:
                self.process_single_mpid(record)
                time.sleep(self.mpid_sleep_seconds)
            except Exception as exc:
                logger.exception("Unexpected error while processing MPID %s: %s", record.get("MPID"), exc)

        logger.info("Scraper finished")


if __name__ == "__main__":
    CREDENTIALS_FILE = "credentials.json"
    SPREADSHEET_NAME = "Copy of N8N - Project All Records"
    PROXY_CONFIG = None
    MAX_MPIDS_PER_RUN = 10

    scraper = FINRABrokerCheckScraper(
        google_sheets_creds_file=CREDENTIALS_FILE,
        spreadsheet_name=SPREADSHEET_NAME,
        proxy=PROXY_CONFIG,
    )
    scraper.run(max_mpids=MAX_MPIDS_PER_RUN)

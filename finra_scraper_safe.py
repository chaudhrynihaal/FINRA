import logging
import time
import json
import os
import argparse
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
    MAX_CELL_CHARS = 50000
    SAFE_CELL_CHARS = 49000

    @staticmethod
    def _coerce_cell_value(value) -> str:
        """Google Sheets cells can't receive Python lists/dicts; coerce to scalar."""
        if value is None:
            return ""
        if isinstance(value, (list, dict)):
            try:
                out = json.dumps(value, ensure_ascii=False)
            except Exception:
                out = str(value)
        else:
            # Keep primitives as strings/numbers; API expects cell scalar values.
            out = str(value)

        # Google Sheets rejects single-cell values over 50k chars.
        if len(out) > FINRABrokerCheckScraper.MAX_CELL_CHARS:
            marker = "... [TRUNCATED]"
            trim_to = max(FINRABrokerCheckScraper.SAFE_CELL_CHARS - len(marker), 0)
            return out[:trim_to] + marker
        return out

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
        self.master_write_spreadsheet = None
        self.master_spreadsheet_rollover_index = 1
        # Safety: keep rebalance opt-in only (it rewrites sheet ranges).
        self.enable_master_rebalance = False
        # Avoid expensive full-sheet reads on very large tabs.
        self.compact_max_grid_cells = 300000

        # Tunables to speed up runs while still being gentle on APIs.
        self.page_sleep_seconds = 0.25
        self.mpid_sleep_seconds = 1.0
        # Clear temp sheet after each MPID to keep workbook cell use low.
        self.temp_clear_every_n_mpids = 1

        # Cached sheet metadata/state to avoid re-reading large sheets per MPID.
        self.mpid_col_index: Optional[int] = None
        self.flag_col_index: Optional[int] = None  # 0-based, for MPID sheet
        # Note: avoid set[str] typing for older Python versions.
        self.master_existing_ids: Optional[set] = None

    def _workbook_cell_usage(self) -> Tuple[int, int]:
        """
        Return (allocated_cells, sheet_count) across the workbook.
        Allocated cells are based on sheet grid size (rows * cols), not filled values.
        """
        total_cells = 0
        sheet_count = 0
        try:
            for ws in self.spreadsheet.worksheets():
                total_cells += int(ws.row_count) * int(ws.col_count)
                sheet_count += 1
        except Exception as exc:
            logger.warning("Could not compute workbook cell usage: %s", exc)
        return total_cells, sheet_count

    @staticmethod
    def _spreadsheet_cell_usage(spreadsheet) -> Tuple[int, int]:
        """Return (allocated_cells, sheet_count) for a specific spreadsheet."""
        total_cells = 0
        sheet_count = 0
        try:
            for ws in spreadsheet.worksheets():
                total_cells += int(ws.row_count) * int(ws.col_count)
                sheet_count += 1
        except Exception:
            return 0, 0
        return total_cells, sheet_count

    @staticmethod
    def _is_cell_limit_error(exc: Exception) -> bool:
        return "increase the number of cells in the workbook above the limit" in str(exc).lower()

    def _can_expand_spreadsheet(self, spreadsheet, additional_cells: int) -> bool:
        """Check if adding cells would stay under Google Sheets 10M workbook cap."""
        if additional_cells <= 0:
            return True
        allocated_cells, _ = self._spreadsheet_cell_usage(spreadsheet)
        return (allocated_cells + additional_cells) <= 10000000

    @staticmethod
    def _col_to_a1(col_num: int) -> str:
        """Convert 1-based column index to A1 column letters."""
        result = ""
        n = col_num
        while n > 0:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result

    def _ensure_master_sheet_in_spreadsheet(self, spreadsheet) -> Optional[gspread.Worksheet]:
        """Get/create master worksheet in a target spreadsheet and ensure headers exist."""
        try:
            worksheet = spreadsheet.worksheet(self.individual_sheet_name)
        except Exception:
            worksheet = spreadsheet.add_worksheet(
                title=self.individual_sheet_name,
                rows=1000,
                cols=max(len(self.RECORD_COLUMNS), 1),
            )
            logger.info(
                "Created master worksheet '%s' in spreadsheet '%s'",
                self.individual_sheet_name,
                spreadsheet.title,
            )

        try:
            header = worksheet.row_values(1)
            if not header or len(header) < len(self.RECORD_COLUMNS):
                worksheet.update("A1", [self.RECORD_COLUMNS])
                logger.info("Initialized master worksheet headers in '%s'", spreadsheet.title)
        except Exception as exc:
            logger.error("Failed to initialize master headers in '%s': %s", spreadsheet.title, exc)
            return None
        return worksheet

    def _load_master_existing_ids_from_target(self) -> None:
        """Refresh dedupe cache from current master write target."""
        self.master_existing_ids = set()
        if self.master_write_spreadsheet is None:
            return
        try:
            ws = self.master_write_spreadsheet.worksheet(self.individual_sheet_name)
            master_df = get_as_dataframe(ws, evaluate_formulas=False)
            if "ind_source_id" in master_df.columns:
                self.master_existing_ids = set(master_df["ind_source_id"].dropna().astype(str))
        except Exception as exc:
            logger.warning("Could not load master IDs from rollover target: %s", exc)
            self.master_existing_ids = set()

    def _activate_master_rollover_target(self, rollover_index: int) -> bool:
        """
        Switch master writes to '<base title> - N' spreadsheet.
        Index 1 means original spreadsheet title.
        """
        if self.client is None:
            return False

        base_name = self.spreadsheet_name
        target_name = base_name if rollover_index == 1 else f"{base_name} - {rollover_index}"

        try:
            target_spreadsheet = self.client.open(target_name)
        except Exception:
            try:
                target_spreadsheet = self.client.create(target_name)
                logger.info("Created rollover spreadsheet: %s", target_name)
            except Exception as exc:
                logger.error("Failed to open/create rollover spreadsheet '%s': %s", target_name, exc)
                return False

        ws = self._ensure_master_sheet_in_spreadsheet(target_spreadsheet)
        if ws is None:
            return False

        self.master_write_spreadsheet = target_spreadsheet
        self.master_spreadsheet_rollover_index = rollover_index
        self._load_master_existing_ids_from_target()
        logger.info("Master write target set to spreadsheet '%s'", target_spreadsheet.title)
        return True

    def _rollover_master_to_next_spreadsheet(self) -> bool:
        """Move master writes to next numbered spreadsheet target."""
        next_index = self.master_spreadsheet_rollover_index + 1
        logger.warning(
            "Rolling over master writes from '%s' to next spreadsheet index %s",
            self.master_write_spreadsheet.title if self.master_write_spreadsheet else self.spreadsheet_name,
            next_index,
        )
        return self._activate_master_rollover_target(next_index)

    def rebalance_master_sheet_space(self) -> None:
        """
        Move master data directly under header so free rows remain at the end.
        This handles cases where many empty rows were inserted near the top.
        """
        try:
            if self.master_write_spreadsheet is None:
                self.master_write_spreadsheet = self.spreadsheet
            worksheet = self.master_write_spreadsheet.worksheet(self.individual_sheet_name)
            values = worksheet.get_all_values()
            if not values:
                return

            header = values[0]
            raw_data_rows = values[1:]
            compact_data_rows = [row for row in raw_data_rows if any(str(cell).strip() for cell in row)]

            # Already compact (no sparse/leading empty rows in used range).
            if len(compact_data_rows) == len(raw_data_rows):
                return

            current_rows = int(worksheet.row_count)
            current_cols = int(worksheet.col_count)
            data_start_row = 2
            data_end_row = max(current_rows, 2)
            clear_range = f"A{data_start_row}:{self._col_to_a1(current_cols)}{data_end_row}"
            worksheet.batch_clear([clear_range])

            # Keep header as-is if present, otherwise initialize it.
            if not any(str(cell).strip() for cell in header):
                worksheet.update("A1", [self.RECORD_COLUMNS])

            if compact_data_rows:
                end_col = self._col_to_a1(max(len(compact_data_rows[0]), 1))
                end_row = data_start_row + len(compact_data_rows) - 1
                write_range = f"A{data_start_row}:{end_col}{end_row}"
                worksheet.update(
                    range_name=write_range,
                    values=compact_data_rows,
                    value_input_option="USER_ENTERED",
                )

            logger.info(
                "Rebalanced master sheet layout: %s data row(s) compacted under header; free rows are now at the end",
                len(compact_data_rows),
            )
        except Exception as exc:
            logger.warning("Could not rebalance master sheet space: %s", exc)

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
        self.master_write_spreadsheet = self.spreadsheet
        self.master_spreadsheet_rollover_index = 1
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
            f"?query={mpid}&includePrevious=true&nrows={nrows}&start={start}&wt=json&filter=active=true"
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
            # Guard against implicit grid growth when workbook is near 10M cells.
            current_rows = int(worksheet.row_count)
            current_cols = int(worksheet.col_count)
            required_cols = len(self.RECORD_COLUMNS)
            if required_cols > current_cols:
                add_cols = required_cols - current_cols
                additional_cells = current_rows * add_cols
                if self._can_expand_spreadsheet(self.spreadsheet, additional_cells):
                    worksheet.resize(rows=current_rows, cols=required_cols)
                    current_cols = required_cols
                    logger.info("Expanded temp sheet columns to %s", required_cols)
                else:
                    logger.error(
                        "Temp sheet has %s col(s), needs %s; cannot expand due to workbook cell cap",
                        current_cols,
                        required_cols,
                    )
                    return

            # Estimate the next write row without loading the full worksheet.
            non_empty_in_col_a = worksheet.col_values(1)
            next_row = len(non_empty_in_col_a) + 1  # 1-based, next free row
            available_rows = max(current_rows - next_row + 1, 0)
            if len(rows) > available_rows:
                needed_rows = len(rows) - available_rows
                additional_cells = needed_rows * current_cols
                if self._can_expand_spreadsheet(self.spreadsheet, additional_cells):
                    worksheet.resize(rows=current_rows + needed_rows, cols=current_cols)
                    logger.info(
                        "Expanded temp sheet by %s row(s) to fit append batch",
                        needed_rows,
                    )
                else:
                    allocated_cells, _ = self._workbook_cell_usage()
                    logger.error(
                        "Temp append needs %s additional row(s), but only %s row(s) available in current grid. "
                        "Workbook allocated cells: %s/10000000. Skipping temp append.",
                        needed_rows,
                        available_rows,
                        allocated_cells,
                    )
                    return

            # append_rows avoids re-reading the full sheet to find next_row.
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")
            logger.info("Appended %s record(s) to temp sheet", len(records))
        except Exception as exc:
            logger.error("Error updating temp sheet (append_rows): %s", exc)
            # Fallback to previous behavior (slower but more compatible).
            try:
                if worksheet is None:
                    worksheet = self.spreadsheet.worksheet(self.temp_sheet_name)
                # Avoid fallback path if it would require any worksheet growth.
                current_rows = int(worksheet.row_count)
                current_cols = int(worksheet.col_count)
                required_cols = len(self.RECORD_COLUMNS)
                if required_cols > current_cols:
                    logger.error(
                        "Fallback skipped: temp sheet columns insufficient (%s < %s) and expansion is blocked",
                        current_cols,
                        required_cols,
                    )
                    return
                existing_df = get_as_dataframe(worksheet, evaluate_formulas=False)
                next_row = len(existing_df) + 2
                rows_needed = len(records)
                available_rows = max(current_rows - next_row + 1, 0)
                if rows_needed > available_rows:
                    logger.error(
                        "Fallback skipped: temp sheet has no room for %s row(s) without resizing",
                        rows_needed,
                    )
                    return
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

    def clear_temp_sheet(self) -> None:
        """
        Clear all data rows from temp sheet while preserving header row,
        then shrink worksheet dimensions to reduce allocated cell count.
        This helps prevent hitting the workbook cell limit over time.
        """
        try:
            worksheet = self.spreadsheet.worksheet(self.temp_sheet_name)
            values = worksheet.get_all_values()
            row_count = len(values)

            # Nothing to clear or only header exists.
            if row_count <= 1:
                logger.info("Temp sheet is already empty (data rows).")
                return

            # Clear rows 2..last only, keep row 1 headers.
            clear_range = f"2:{row_count}"
            worksheet.batch_clear([clear_range])
            logger.info("Cleared %s data row(s) from temp sheet", row_count - 1)

            # Important: batch_clear does not reduce allocated grid size.
            # Resize temp sheet to keep only header + one empty row and
            # the required output columns.
            # Never increase dimensions here; only shrink.
            target_cols = max(1, min(int(worksheet.col_count), len(self.RECORD_COLUMNS)))
            worksheet.resize(rows=2, cols=target_cols)
            logger.info("Resized temp sheet to 2 rows x %s cols", target_cols)
        except Exception as exc:
            logger.error("Error clearing temp sheet: %s", exc)

    def compact_sheet_dimensions(self, sheet_name: str) -> None:
        """
        Resize a worksheet to tightly fit current data (with a minimal floor).
        This can free allocated cells that count toward the 10M workbook limit.
        """
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            current_rows = int(worksheet.row_count)
            current_cols = int(worksheet.col_count)
            grid_cells = current_rows * current_cols

            # On very large sheets, get_all_values() can appear stuck for minutes.
            if grid_cells > self.compact_max_grid_cells:
                logger.info(
                    "Skipping deep compaction for '%s' due to large grid (%s cells > %s threshold)",
                    sheet_name,
                    grid_cells,
                    self.compact_max_grid_cells,
                )
                return

            values = worksheet.get_all_values()

            used_rows = max(len(values), 1)
            used_cols = 1
            for row in values:
                if len(row) > used_cols:
                    used_cols = len(row)

            # Keep practical minimum rows, but never increase dimensions in compact mode.
            if sheet_name == self.temp_sheet_name:
                used_rows = max(used_rows, 2)
                used_cols = max(used_cols, 1)
            elif sheet_name == self.individual_sheet_name:
                used_rows = max(used_rows, 1)
                used_cols = max(used_cols, 1)
            else:
                used_rows = max(used_rows, 1)
                used_cols = max(used_cols, 1)

            used_rows = min(used_rows, current_rows)
            used_cols = min(used_cols, current_cols)

            worksheet.resize(rows=used_rows, cols=used_cols)
            logger.info("Compacted '%s' to %s rows x %s cols", sheet_name, used_rows, used_cols)
        except Exception as exc:
            logger.warning("Could not compact '%s': %s", sheet_name, exc)

    def update_master_sheet(self, records: List[Dict], rollover_attempt: int = 0) -> bool:
        if not records:
            return True
        if rollover_attempt > 10:
            logger.error("Master rollover exceeded maximum attempts; aborting write")
            return False
        try:
            if self.master_write_spreadsheet is None:
                self.master_write_spreadsheet = self.spreadsheet
            worksheet = self.master_write_spreadsheet.worksheet(self.individual_sheet_name)
            if self.master_existing_ids is None:
                self.master_existing_ids = set()

            new_records = []
            pending_new_ids = []
            for row in records:
                rid = row.get("ind_source_id")
                if not rid:
                    continue
                rid_str = str(rid)
                if rid_str in self.master_existing_ids:
                    continue
                new_records.append(row)
                pending_new_ids.append(rid_str)

            if not new_records:
                logger.info("No new records to append to master sheet")
                return True

            rows = [
                [self._coerce_cell_value(rec.get(col)) for col in self.RECORD_COLUMNS]
                for rec in new_records
            ]

            current_rows = int(worksheet.row_count)
            current_cols = int(worksheet.col_count)
            required_cols = len(self.RECORD_COLUMNS)
            if required_cols > current_cols:
                add_cols = required_cols - current_cols
                additional_cells = current_rows * add_cols
                if self._can_expand_spreadsheet(self.master_write_spreadsheet, additional_cells):
                    worksheet.resize(rows=current_rows, cols=required_cols)
                    current_cols = required_cols
                    logger.info(
                        "Expanded master sheet columns to %s in '%s'",
                        required_cols,
                        self.master_write_spreadsheet.title,
                    )
                else:
                    allocated_cells, _ = self._workbook_cell_usage()
                    logger.error(
                        "Master append blocked: sheet has %s col(s), needs %s. "
                        "Workbook allocated cells (primary): %s/10000000.",
                        current_cols,
                        required_cols,
                        allocated_cells,
                    )
                    if self._rollover_master_to_next_spreadsheet():
                        return self.update_master_sheet(records, rollover_attempt=rollover_attempt + 1)
                    return False

            non_empty_in_col_a = worksheet.col_values(1)
            next_row = len(non_empty_in_col_a) + 1
            available_rows = max(current_rows - next_row + 1, 0)
            if len(rows) > available_rows:
                needed_rows = len(rows) - available_rows
                additional_cells = needed_rows * current_cols
                if self._can_expand_spreadsheet(self.master_write_spreadsheet, additional_cells):
                    worksheet.resize(rows=current_rows + needed_rows, cols=current_cols)
                    logger.info(
                        "Expanded master sheet by %s row(s) in '%s' to fit append batch",
                        needed_rows,
                        self.master_write_spreadsheet.title,
                    )
                else:
                    allocated_cells, _ = self._workbook_cell_usage()
                    logger.error(
                        "Master append needs %s additional row(s), but only %s row(s) available in current grid. "
                        "Workbook allocated cells (primary): %s/10000000.",
                        needed_rows,
                        available_rows,
                        allocated_cells,
                    )
                    if self._rollover_master_to_next_spreadsheet():
                        return self.update_master_sheet(records, rollover_attempt=rollover_attempt + 1)
                    return False

            worksheet.append_rows(rows, value_input_option="USER_ENTERED")
            for rid_str in pending_new_ids:
                self.master_existing_ids.add(rid_str)
            logger.info("Appended %s new record(s) to master sheet", len(new_records))
            return True
        except Exception as exc:
            logger.error("Error updating master sheet: %s", exc)
            if self._is_cell_limit_error(exc) and self._rollover_master_to_next_spreadsheet():
                return self.update_master_sheet(records, rollover_attempt=rollover_attempt + 1)
            return False

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

    def process_single_mpid(self, mpid_info: Dict) -> bool:
        mpid = mpid_info["MPID"]
        row = mpid_info["row"]
        logger.info("Processing MPID %s (row %s)", mpid, row)

        initial_data = self.fetch_individual_data(mpid)
        if not initial_data:
            logger.error("Initial fetch failed for MPID %s", mpid)
            self.update_mpid_flag(mpid, row)
            return True

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
            return True

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
        master_ok = self.update_master_sheet(unique_records)
        if not master_ok:
            logger.error("Skipping MPID flag update for %s because master write failed", mpid)
            return False
        if self.master_write_spreadsheet is not None:
            logger.info(
                "MPID %s master records written to spreadsheet '%s'",
                mpid,
                self.master_write_spreadsheet.title,
            )
        self.update_mpid_flag(mpid, row)
        return True

    def run(self, max_mpids: int = 1) -> None:
        if not self.preflight_check():
            logger.error("Preflight failed. Exiting.")
            return

        # Free allocated cells across tabs before writing new data.
        logger.info("Compaction step 1/3: %s", self.mpid_sheet_name)
        self.compact_sheet_dimensions(self.mpid_sheet_name)
        logger.info("Compaction step 2/3: %s", self.individual_sheet_name)
        self.compact_sheet_dimensions(self.individual_sheet_name)
        logger.info("Compaction step 3/3: %s", self.temp_sheet_name)
        self.compact_sheet_dimensions(self.temp_sheet_name)
        if self.enable_master_rebalance:
            self.rebalance_master_sheet_space()

        # Auto-clean temp sheet before each run to keep workbook size manageable.
        self.clear_temp_sheet()

        mpid_records = self.get_unprocessed_mpids(limit=max_mpids)
        if not mpid_records:
            logger.info("No unprocessed MPIDs found")
            return

        for idx, record in enumerate(mpid_records, start=1):
            try:
                processed_ok = self.process_single_mpid(record)
                if not processed_ok:
                    allocated_cells, _ = self._workbook_cell_usage()
                    logger.error(
                        "Stopping run after MPID %s because a required sheet write failed. "
                        "Allocated cells: %s/10000000.",
                        record.get("MPID"),
                        allocated_cells,
                    )
                    break
                time.sleep(self.mpid_sleep_seconds)
            except Exception as exc:
                logger.exception("Unexpected error while processing MPID %s: %s", record.get("MPID"), exc)

            if (
                self.temp_clear_every_n_mpids > 0
                and idx % self.temp_clear_every_n_mpids == 0
            ):
                self.clear_temp_sheet()
                logger.info(
                    "Temp sheet cleared after %s MPID(s) (checkpoint every %s)",
                    idx,
                    self.temp_clear_every_n_mpids,
                )

        logger.info("Scraper finished")


if __name__ == "__main__":
    CREDENTIALS_FILE = "credentials.json"
    SPREADSHEET_NAME = "Copy of N8N - Project All Records - 2"
    PROXY_CONFIG = None
    MAX_MPIDS_PER_RUN = 50
    TEMP_CLEAR_EVERY_N_MPIDS = int(os.getenv("TEMP_CLEAR_EVERY_N_MPIDS", "1"))

    parser = argparse.ArgumentParser(description="Run FINRA BrokerCheck scraper")
    parser.add_argument(
        "--max-mpids",
        type=int,
        default=MAX_MPIDS_PER_RUN,
        help="Maximum number of unprocessed MPIDs to process in one run",
    )
    parser.add_argument(
        "--temp-clear-every",
        type=int,
        default=TEMP_CLEAR_EVERY_N_MPIDS,
        help="Clear temp sheet after every N processed MPIDs (1 = every MPID)",
    )
    args = parser.parse_args()

    scraper = FINRABrokerCheckScraper(
        google_sheets_creds_file=CREDENTIALS_FILE,
        spreadsheet_name=SPREADSHEET_NAME,
        proxy=PROXY_CONFIG,
    )
    scraper.temp_clear_every_n_mpids = max(1, args.temp_clear_every)
    logger.info(
        "Runtime options: max_mpids=%s, temp_clear_every_n_mpids=%s",
        args.max_mpids,
        scraper.temp_clear_every_n_mpids,
    )
    scraper.run(max_mpids=args.max_mpids)

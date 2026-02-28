"""
Standalone script to extract Conversion % trend from source sheet
Extracts Conversion % data for specified hubs and displays latest 7 days in destination sheet
"""

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from whatsapp_sheet_image import send_sheet_range_to_whatsapp
import pandas as pd
import numpy as np
import logging
import time
import string
import requests
import google.auth.exceptions
import urllib3
from datetime import datetime, timedelta, date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SERVICE_ACCOUNT_FILE = 'service_account_key.json'

# Conversion % Source Sheet Configuration (same as ncd_breach_trend_analyzer.py)
CONVERSION_SOURCE_SPREADSHEET_ID = '1OT_fTFCiPpRuokJRPrfpx1PiKV1syyXdNN42KaP_fCQ'

# Conversion % Destination Sheet Configuration
CONVERSION_DEST_SPREADSHEET_ID = '1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM'
CONVERSION_DEST_WORKSHEET_NAME = 'Conversion'
CONVERSION_DAYS_TO_FETCH = 15  # Latest 15 days

# Target Hub Names for Conversion % extraction
TARGET_HUB_NAMES = [
    'BidarFortHub_BDR',
    'KoorieeSoukyaRdODH_BLR',
    'NaubadMDH_BDR',
    'LargeLogicDharapuramODH_DHP',
    'LargeLogicKuniyamuthurODH_CJB',
    'KoorieeHayathnagarODH_HYD',
    'LargelogicChinnamanurODH_CNM',
    'LargeLogicRameswaramODH_RMS',
    'ElasticRunBidarODH_BDR',
    'TTSPLKodaikanalODH_KDI',
    'HulimavuHub_BLR',
    'SaidabadSplitODH_HYD',
    'BagaluruMDH_BAG',
    'CABTSRNagarODH_HYD',
    'DommasandraSplitODH_DMN',
    'VadipattiMDH_VDP',
    'ThavarekereMDH_THK',
    'KoorieeSoukyaRdTempODH_BLR',
    'TTSPLBatlagunduODH_BGU',
    'SITICSWadiODH_WDI',
    'SulebeleMDH_SUL'
]

# Hub Name to Volume Weight mapping
HUB_VOLUME_WEIGHT_MAPPING = {
    'KoorieeSoukyaRdODH_BLR': 16.24,
    'DommasandraSplitODH_DMN': 8.65,
    'LargeLogicKuniyamuthurODH_CJB': 8.40,
    'HulimavuHub_BLR': 7.06,
    'CABTSRNagarODH_HYD': 6.52,
    'KoorieeHayathnagarODH_HYD': 5.86,
    'BidarFortHub_BDR': 5.53,
    'SaidabadSplitODH_HYD': 5.22,
    'TTSPLBatlagunduODH_BGU': 4.06,
    'LargelogicChinnamanurODH_CNM': 3.59,
    'ElasticRunBidarODH_BDR': 3.35,
    'SulebeleMDH_SUL': 3.14,
    'LargeLogicDharapuramODH_DHP': 3.12,
    'SITICSWadiODH_WDI': 2.88,
    'BagaluruMDH_BAG': 2.67,
    'NaubadMDH_BDR': 2.60,
    'ThavarekereMDH_THK': 2.44,
    'LargeLogicRameswaramODH_RMS': 2.24,
    'TTSPLKodaikanalODH_KDI': 2.22,
    'VadipattiMDH_VDP': 2.14,
    'KoorieeSoukyaRdTempODH_BLR': 0.96
}

# Hub Name to CLM Name and State mapping (same as ncd_breach_trend_analyzer.py)
# Singaram hubs: LARGELOGICCHINNAMANURODH_CNM, TTSPLKODAIKANALODH_KDI, LARGELOGICKUNIYAMUTHURODH_CJB,
#                VadipattiMDH_VDP, TTSPLBATLAGUNDUODH_BGU
HUB_CLM_STATE_MAPPING = {
    # Singaram (Tamil Nadu)
    'LARGELOGICCHINNAMANURODH_CNM': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'LargelogicChinnamanurODH_CNM': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'LargeLogicChinnamanurODH_CNM': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'TTSPLKODAIKANALODH_KDI': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'TTSPLKodaikanalODH_KDI': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'LARGELOGICKUNIYAMUTHURODH_CJB': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'LargeLogicKuniyamuthurODH_CJB': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'VadipattiMDH_VDP': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'TTSPLBATLAGUNDUODH_BGU': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'TTSPLBatlagunduODH_BGU': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    # Madvesh (Tamil Nadu)
    'LARGELOGICRAMESWARAMODH_RMS': {'CLM Name': 'Madvesh', 'State': 'Tamil Nadu'},
    'LargeLogicRameswaramODH_RMS': {'CLM Name': 'Madvesh', 'State': 'Tamil Nadu'},
    'LARGELOGICDHARAPURAMODH_DHP': {'CLM Name': 'Madvesh', 'State': 'Tamil Nadu'},
    'LargeLogicDharapuramODH_DHP': {'CLM Name': 'Madvesh', 'State': 'Tamil Nadu'},
    # Karnataka & Telangana
    'BagaluruMDH_BAG': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'ElasticRunBidarODH_BDR': {'CLM Name': 'Haseem', 'State': 'Karnataka'},
    'SITICSWadiODH_WDI': {'CLM Name': 'Haseem', 'State': 'Karnataka'},
    'saidabadsplitODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'SaidabadSplitODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'HulimavuHub_BLR': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'ThavarekereMDH_THK': {'CLM Name': 'Irappa', 'State': 'Karnataka'},
    'KoorieeSoukyaRdTempODH_BLR': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'NaubadMDH_BDR': {'CLM Name': 'Haseem', 'State': 'Karnataka'},
    'KOORIEEHAYATHNAGARODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'KoorieeHayathnagarODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'DommasandraSplitODH_DMN': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'KoorieeSoukyaRdODH_BLR': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'BidarFortHub_BDR': {'CLM Name': 'Haseem', 'State': 'Karnataka'},
    'CABTSRNAGARODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'CABTSRNagarODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'SulebeleMDH_SUL': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
}

# Pre-built normalized lookup for reliable case-insensitive matching (source sheet hub names may vary)
def _build_hub_clm_normalized_lookup():
    d = {}
    for hub_key, clm_state in HUB_CLM_STATE_MAPPING.items():
        norm = str(hub_key).strip().lower() if hub_key else ""
        if norm:
            d[norm] = clm_state
    return d

_HUB_CLM_NORMALIZED_LOOKUP = _build_hub_clm_normalized_lookup()

# Google Sheets scopes
SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]


def parse_date(date_str):
    """Parse date string in various formats and return date object"""
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    if not date_str:
        return None
    
    # Try Excel serial number first
    try:
        if date_str.replace('.', '').replace('-', '').replace('/', '').isdigit():
            excel_date = float(date_str)
            if excel_date > 59:
                excel_date -= 1
            excel_epoch = datetime(1899, 12, 30)
            parsed_datetime = excel_epoch + timedelta(days=excel_date)
            return parsed_datetime.date()
    except:
        pass
    
    # Remove time portion if present
    if ' ' in date_str:
        date_str = date_str.split(' ')[0]
    
    # Try common date formats (prioritize %d-%b format for dates like "16-Dec")
    date_formats = [
        '%d-%b',      # 16-Dec (most common format in source sheet)
        '%d-%B',      # 16-December
        '%d-%b-%Y',   # 16-Dec-2024
        '%d-%b-%y',   # 16-Dec-24
        '%d-%B-%Y',   # 16-December-2024
        '%Y-%m-%d',   # 2024-12-16
        '%d-%m-%Y',   # 16-12-2024
        '%m/%d/%Y',   # 12/16/2024
        '%d/%m/%Y',   # 16/12/2024
        '%Y/%m/%d',   # 2024/12/16
        '%d.%m.%Y',   # 16.12.2024
        '%m-%d-%Y',   # 12-16-2024
        '%d/%m/%y',   # 16/12/24
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            result_date = parsed_date.date()
            
            # Handle formats without year (like "16-Dec") - use current year, but handle year wrapping
            if fmt == '%d-%b' or fmt == '%d-%B':
                current_year = datetime.now().year
                if result_date.year == 1900:
                    try:
                        result_date = datetime(current_year, result_date.month, result_date.day).date()
                        # If the date is more than 30 days in the future, assume it's from the previous year
                        # (e.g., if today is Jan 1, 2025, "24-Dec" should be Dec 24, 2024, not Dec 24, 2025)
                        today = datetime.now().date()
                        days_ahead = (result_date - today).days
                        if days_ahead > 30:
                            result_date = datetime(current_year - 1, result_date.month, result_date.day).date()
                    except ValueError:
                        # Handle leap year edge case (Feb 29)
                        result_date = datetime(current_year, result_date.month, min(result_date.day, 28)).date()
            
            return result_date
        except (ValueError, TypeError):
            continue
    
    # Try pandas to_datetime as fallback
    try:
        parsed = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(parsed):
            return parsed.date()
    except:
        pass
    
    return None


def get_google_sheets_client():
    """Initialize and return Google Sheets client with retry logic for network/SSL errors"""
    max_retries = 3
    retry_delay = 5  # Start with 5 seconds
    
    for attempt in range(max_retries):
        try:
            logger.info("🔑 Setting up Google Sheets connection...")
            if attempt > 0:
                logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries}...")
                time.sleep(retry_delay * attempt)  # Exponential backoff: 0s, 5s, 10s
            
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            client = gspread.authorize(creds)
            logger.info("✅ Google Sheets client initialized successfully")
            
            # Display service account email for sharing reference
            import json
            with open(SERVICE_ACCOUNT_FILE, 'r') as f:
                service_account_data = json.load(f)
                service_account_email = service_account_data.get('client_email', 'Not found')
                logger.info(f"📧 Service Account Email: {service_account_email}")
                logger.info("💡 Make sure the Google Sheet is shared with this email address (Editor access)")
            
            return client
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, 
                google.auth.exceptions.TransportError, urllib3.exceptions.SSLError,
                requests.exceptions.Timeout, urllib3.exceptions.NameResolutionError) as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (attempt + 1)
                logger.warning(f"⚠️ Network/connection error (attempt {attempt + 1}/{max_retries}): {type(e).__name__}")
                logger.warning(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"❌ Failed to initialize Google Sheets client after {max_retries} attempts")
                logger.error(f"❌ Last error: {type(e).__name__}: {e}")
                logger.error("💡 This appears to be a network/connectivity issue.")
                logger.error("💡 Please check:")
                logger.error("   1. Your internet connection")
                logger.error("   2. DNS resolution (can you resolve sheets.googleapis.com?)")
                logger.error("   3. Firewall/proxy settings")
                logger.error("   4. Antivirus software blocking connections")
                logger.error("   5. Corporate network restrictions")
                raise
        except Exception as e:
            logger.error(f"❌ Error initializing Google Sheets client: {e}")
            raise


def retry_api_call(func, *args, max_retries=3, **kwargs):
    """Helper function to retry API calls with exponential backoff for rate limit and service unavailable errors"""
    retry_delay = 60  # Start with 60 seconds
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            status_code = e.response.status_code if hasattr(e, 'response') and hasattr(e.response, 'status_code') else None
            # Retry for 429 (rate limit) and 503 (service unavailable) errors
            if status_code in [429, 503] and attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff: 60s, 120s, 240s
                error_type = "Rate limit exceeded" if status_code == 429 else "Service unavailable"
                logger.warning(f"⚠️ {error_type} ({status_code}). Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
            else:
                raise  # Re-raise if not a retryable error or out of retries
        except Exception as e:
            raise  # Re-raise non-API errors immediately


def get_column_letter(col_num):
    """Convert column number (1-based) to letter(s) (A, B, ..., Z, AA, AB, ...)"""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = string.ascii_uppercase[col_num % 26] + result
        col_num //= 26
    return result


def clear_range(worksheet, start_row, end_row, num_cols):
    """Clear only a specific range in the worksheet, preserving other cells"""
    if num_cols == 0 or end_row < start_row:
        return
    
    start_col_letter = 'A'
    end_col_letter = get_column_letter(num_cols)
    range_str = f'{start_col_letter}{start_row}:{end_col_letter}{end_row}'
    
    try:
        worksheet.batch_clear([range_str])
        logger.info(f"🧹 Cleared range {range_str} (preserving other cells)")
    except Exception as e:
        logger.warning(f"⚠️ Could not clear range {range_str}: {e}")


def find_hub_name_column(headers):
    """Find the 'Hub Name' column index - try various possible names"""
    # Exclude these from being considered as Hub Name
    exclude_as_hub_name = ['zone', 'zonal head', 'threshold', 'total', 'summary']
    
    # Try exact match first (with space, underscore, or variations)
    for idx, header in enumerate(headers):
        if not header:
            continue
        header_str = str(header).strip()
        header_lower = header_str.lower()
        # Skip if it's in exclude list
        if header_lower in exclude_as_hub_name:
            continue
        # Check for various hub name formats
        if (header_lower == 'hub name' or header_lower == 'hub_name' or 
            header_lower == 'hub' or header_lower.replace('_', ' ') == 'hub name' or
            header_lower.replace(' ', '_') == 'hub_name'):
            return idx
    
    # If not found, check if first column might be hub name (but not if it's in exclude list)
    if len(headers) > 0 and headers[0]:
        first_col = str(headers[0]).strip()
        first_col_lower = first_col.lower()
        # Don't use if it's in exclude list
        if first_col_lower in exclude_as_hub_name:
            return None
        # Check if first column looks like a hub name (not a date, not a percentage, not a number)
        if '%' not in first_col and not first_col.replace('.', '').replace('-', '').isdigit():
            try:
                parse_date(first_col)
                # If it's a date, don't use it
            except:
                # Not a date, might be hub name
                return 0
    
    return None


def read_conversion_source_sheet(client):
    """Read data from the Conversion % source sheet (same as ncd_breach_trend_analyzer.py)"""
    try:
        logger.info(f"📊 Opening Conversion % source spreadsheet: {CONVERSION_SOURCE_SPREADSHEET_ID}")
        spreadsheet = client.open_by_key(CONVERSION_SOURCE_SPREADSHEET_ID)
        logger.info(f"✅ Opened spreadsheet: {spreadsheet.title}")
        
        # Use "Base Data" worksheet (same as ncd_breach_trend_analyzer.py)
        # This is the primary worksheet that contains all metrics including Conv %
        worksheet_name = "Base Data"
        worksheet = None
        
        # First, try exact match for "Base Data"
        for ws in spreadsheet.worksheets():
            if ws.title == "Base Data":
                worksheet = ws
                logger.info(f"✅ Found worksheet: {worksheet.title}")
                break
        
        # If not found, try case-insensitive match
        if not worksheet:
            for ws in spreadsheet.worksheets():
                if ws.title.lower() == worksheet_name.lower():
                    worksheet = ws
                    logger.info(f"✅ Found worksheet: {worksheet.title}")
                    break
        
        # Fallback: try to find worksheet that contains "Base" in the name (but not "Reservations")
        if not worksheet:
            for ws in spreadsheet.worksheets():
                if 'Base' in ws.title and 'Reservation' not in ws.title:
                    worksheet = ws
                    logger.info(f"✅ Found worksheet by name: {worksheet.title}")
                    break
        
        if not worksheet:
            logger.error(f"❌ Worksheet '{worksheet_name}' or alternatives not found!")
            logger.info(f"Available worksheets: {[ws.title for ws in spreadsheet.worksheets()]}")
            return None, None
        
        logger.info(f"✅ Using worksheet: {worksheet.title}")
        logger.info(f"   Rows: {worksheet.row_count}, Cols: {worksheet.col_count}")
        
        # Read all data
        logger.info("📖 Reading data from Conversion % source worksheet...")
        values = worksheet.get_all_values()
        
        if not values:
            logger.warning("⚠️ No data found in Conversion % source worksheet")
            return [], worksheet
        
        logger.info(f"✅ Read {len(values)} rows from Conversion % source worksheet")
        
        return values, worksheet
    
    except Exception as e:
        logger.error(f"❌ Error reading Conversion % source worksheet: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise
def convert_conversion_to_dataframe(values):
    """Convert Conversion % worksheet values to pandas DataFrame
    Uses same structure as ncd_breach_trend_analyzer.py (headers in row 2, index 1)
    """
    try:
        if not values:
            logger.warning("⚠️ No data to convert to DataFrame")
            return pd.DataFrame()
        
        # Always use row 2 (index 1) as header row (same as ncd_breach_trend_analyzer.py)
        # This is MANUAL_HEADER_ROW_INDEX = 1
        # DO NOT change this based on where dates are found - the structure is fixed
        header_row_idx = 1
        logger.info(f"📅 Using row {header_row_idx + 1} (index {header_row_idx}) as header row (same as ncd_breach script)")
        
        if len(values) <= header_row_idx:
            logger.warning("⚠️ Not enough rows for headers")
            return pd.DataFrame()
        
        headers = values[header_row_idx]
        
        # Log actual headers for debugging
        logger.info(f"📋 Header row has {len(headers)} columns")
        logger.info(f"📋 First 10 headers: {headers[:10]}")
        
        # Handle empty headers - pandas will create duplicate column names if headers are empty
        # We need to ensure unique column names
        processed_headers = []
        header_counts = {}
        for idx, header in enumerate(headers):
            header_str = str(header).strip() if header else ''
            if not header_str:
                # Generate a unique name for empty headers
                header_str = f"Column_{idx + 1}"
            # Handle duplicate headers
            if header_str in header_counts:
                header_counts[header_str] += 1
                header_str = f"{header_str}_{header_counts[header_str]}"
            else:
                header_counts[header_str] = 0
            processed_headers.append(header_str)
        
        # If dates are in row 2, data starts from row 3 (index 2)
        data_start_idx = header_row_idx + 1
        data = values[data_start_idx:] if len(values) > data_start_idx else []
        
        # Create DataFrame with processed headers
        df = pd.DataFrame(data, columns=processed_headers)
        
        # Store original headers (before processing) for reference
        original_headers_raw = headers
        
        # Store original headers (raw) and processed headers as attributes for reference
        df.attrs['original_headers'] = processed_headers  # Use processed headers for column matching
        df.attrs['original_headers_raw'] = original_headers_raw  # Keep raw headers for reference
        df.attrs['original_values'] = values
        df.attrs['header_row_idx'] = header_row_idx
        
        # Create a mapping from date/header string to column index for easier lookup
        header_to_col_idx = {}
        for idx, header in enumerate(processed_headers):
            header_str = str(header).strip() if header else ''
            if header_str:
                header_to_col_idx[header_str] = idx
        
        df.attrs['header_to_col_idx'] = header_to_col_idx
        
        # Try to identify and rename Hub Name column if it exists with a different name
        hub_name_col_idx = find_hub_name_column(processed_headers)
        if hub_name_col_idx is not None:
            # Rename the column to 'Hub Name' for consistency
            old_name = processed_headers[hub_name_col_idx]
            df = df.rename(columns={old_name: 'Hub Name'})
            # Update the processed headers too
            processed_headers[hub_name_col_idx] = 'Hub Name'
            df.attrs['original_headers'] = processed_headers
            logger.info(f"✅ Identified and renamed column '{old_name}' to 'Hub Name'")
        elif 'Hub Name' not in df.columns and 'Hub_name' in df.columns:
            # Check for 'Hub_name' with underscore
            df = df.rename(columns={'Hub_name': 'Hub Name'})
            # Update processed headers
            for idx, h in enumerate(processed_headers):
                if str(h).strip() == 'Hub_name':
                    processed_headers[idx] = 'Hub Name'
                    break
            df.attrs['original_headers'] = processed_headers
            logger.info(f"✅ Found and renamed 'Hub_name' to 'Hub Name'")
        elif 'Hub Name' not in df.columns:
            # If still not found, try to use first column as Hub Name
            if len(df.columns) > 0:
                first_col = df.columns[0]
                df = df.rename(columns={first_col: 'Hub Name'})
                processed_headers[0] = 'Hub Name'
                df.attrs['original_headers'] = processed_headers
                logger.info(f"✅ Using first column '{first_col}' as 'Hub Name'")
        
        logger.info(f"✅ Converted to DataFrame: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"📋 Columns: {list(df.columns)}")
        
        return df
    
    except Exception as e:
        logger.error(f"❌ Error converting Conversion % data to DataFrame: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise


def generate_drag_down_summary(conversion_df, date_columns):
    """Generate summary of top 3 hubs dragging down Total Conversion based on Volume Weight"""
    try:
        # Extract Volume Weight values (convert from "X%" format to numeric)
        def extract_volume_weight(weight_str):
            if pd.isna(weight_str) or weight_str == '' or weight_str is None:
                return None
            try:
                cleaned = str(weight_str).replace('%', '').strip()
                if cleaned:
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
            return None
        
        # Extract conversion values from date columns
        def extract_percentage(value_str):
            if pd.isna(value_str) or value_str == '' or value_str is None:
                return None
            try:
                cleaned = str(value_str).replace('%', '').replace(',', '').strip()
                if cleaned and cleaned != "N/A":
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
            return None
        
        # Calculate weighted average for each date column
        date_weighted_averages = {}
        for date_col in date_columns:
            conversion_values = []
            weights_for_date = []
            
            for idx, row in conversion_df.iterrows():
                if row['Hub Name'] in ['Total', 'OFD', 'Del+RVP', 'Revenue']:
                    continue  # Skip summary rows
                
                date_value_str = str(row[date_col]) if date_col in row else ''
                conversion_val = extract_percentage(date_value_str)
                volume_weight = extract_volume_weight(row.get('Volume Weight', ''))
                
                if conversion_val is not None and volume_weight is not None:
                    conversion_values.append(conversion_val)
                    weights_for_date.append(volume_weight)
            
            if len(conversion_values) > 0 and len(weights_for_date) > 0:
                weighted_sum = sum(cv * w for cv, w in zip(conversion_values, weights_for_date))
                total_weight = sum(weights_for_date)
                if total_weight > 0:
                    date_weighted_averages[date_col] = weighted_sum / total_weight
        
        # Calculate overall weighted average (average across all dates)
        if len(date_weighted_averages) > 0:
            overall_weighted_avg = sum(date_weighted_averages.values()) / len(date_weighted_averages)
        else:
            overall_weighted_avg = None
        
        if overall_weighted_avg is None:
            logger.warning("⚠️ Could not calculate weighted average for summary")
            return
        
        # Calculate drag-down impact for each hub
        hub_impacts = []
        
        for idx, row in conversion_df.iterrows():
            if row['Hub Name'] == 'Total':
                continue  # Skip Total row
            
            hub_name = row['Hub Name']
            volume_weight = extract_volume_weight(row.get('Volume Weight', ''))
            
            if volume_weight is None:
                continue
            
            # Calculate average conversion across all dates for this hub
            hub_conversions = []
            for date_col in date_columns:
                date_value_str = str(row[date_col]) if date_col in row else ''
                conversion_val = extract_percentage(date_value_str)
                if conversion_val is not None:
                    hub_conversions.append(conversion_val)
            
            if len(hub_conversions) == 0:
                continue
            
            hub_avg_conversion = sum(hub_conversions) / len(hub_conversions)
            
            # Calculate drag-down impact: (weighted_avg - hub_conversion) * volume_weight
            if hub_avg_conversion < overall_weighted_avg:
                conversion_gap = overall_weighted_avg - hub_avg_conversion
                drag_down_impact = conversion_gap * volume_weight
                hub_impacts.append({
                    'hub_name': hub_name,
                    'impact': drag_down_impact
                })
        
        # Sort by impact (highest impact first)
        hub_impacts.sort(key=lambda x: x['impact'], reverse=True)
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("📊 SUMMARY: TOP 3 HUBS DRAGGING DOWN TOTAL CONVERSION")
        logger.info("="*60)
        logger.info(f"Overall Weighted Average Conversion: {overall_weighted_avg:.2f}%")
        logger.info("")
        
        if len(hub_impacts) == 0:
            logger.info("✅ No hubs found to be dragging down the average")
        else:
            top_3 = hub_impacts[:3]
            for i, hub_data in enumerate(top_3, 1):
                logger.info(f"{i}. {hub_data['hub_name']}")
                logger.info(f"   Impact: {hub_data['impact']:.2f}")
                logger.info("")
        
        logger.info("="*60)
        
    except Exception as e:
        logger.warning(f"⚠️ Error generating drag-down summary: {e}")
        import traceback
        logger.debug(f"Full traceback:\n{traceback.format_exc()}")


def create_conversion_trend_table_by_dates(df, target_hub_names=None, days_to_fetch=7):
    """Create Conversion % trend table grouped by Hub Name with latest N days as columns
    Structure: Each row in source has Date, Hub_name, and Conv % columns
    We need to pivot this data so dates become columns
    If target_hub_names is provided, filters data to only those hubs
    Returns: conversion_df DataFrame with daily Conversion % values for latest N days
    """
    try:
        if df.empty:
            logger.warning("⚠️ No data to create Conversion % trend table")
            return pd.DataFrame()
        
        # Get original headers (row 2, index 1)
        original_headers = df.attrs.get('original_headers', None)
        
        # Find the "Date" column and "Conv %" column
        date_col_name = None
        conv_col_name = None
        date_col_idx = None
        conv_col_idx = None
        
        logger.info(f"🔍 Looking for 'Date' and 'Conv %' columns in headers...")
        if original_headers:
            for col_idx, header in enumerate(original_headers):
                header_str = str(header).strip() if header else ''
                header_lower = header_str.lower()
                
                # Find Date column
                if header_str == 'Date' or header_lower == 'date':
                    date_col_name = header_str
                    date_col_idx = col_idx
                    logger.info(f"✅ Found 'Date' column at index {col_idx}: '{header_str}'")
                
                # Find Conv % column
                if header_str == 'Conv %' or ('conv' in header_lower and '%' in header_str):
                    conv_col_name = header_str
                    conv_col_idx = col_idx
                    logger.info(f"✅ Found Conv % column at index {col_idx}: '{header_str}'")
        
        # Also check DataFrame columns
        if not date_col_name:
            if 'Date' in df.columns:
                date_col_name = 'Date'
                date_col_idx = list(df.columns).index('Date')
                logger.info(f"✅ Found 'Date' column in DataFrame")
        
        if not conv_col_name:
            for col in df.columns:
                col_str = str(col).strip()
                col_lower = col_str.lower()
                if col_str == 'Conv %' or ('conv' in col_lower and '%' in col_str):
                    conv_col_name = col_str
                    conv_col_idx = list(df.columns).index(col)
                    logger.info(f"✅ Found Conv % column in DataFrame: '{col_str}'")
                    break
        
        if not date_col_name:
            logger.error("❌ 'Date' column not found")
            logger.info(f"Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        if not conv_col_name:
            logger.error("❌ 'Conv %' column not found")
            logger.info(f"Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        logger.info(f"📊 Using Date column: '{date_col_name}', Conv % column: '{conv_col_name}'")
        
        # Check if Hub Name column exists (might be 'Hub_name' or 'Hub Name')
        hub_name_col = None
        if 'Hub Name' in df.columns:
            hub_name_col = 'Hub Name'
        elif 'Hub_name' in df.columns:
            hub_name_col = 'Hub_name'
            df = df.rename(columns={'Hub_name': 'Hub Name'})
        else:
            logger.error("❌ 'Hub Name' or 'Hub_name' column not found")
            logger.info(f"Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        # Parse dates from Date column and filter to latest N days
        logger.info(f"📅 Parsing dates from '{date_col_name}' column...")
        df['_parsed_date'] = df[date_col_name].apply(parse_date)
        
        # Filter out rows with invalid dates
        valid_dates_df = df[df['_parsed_date'].notna()].copy()
        if len(valid_dates_df) == 0:
            logger.warning("⚠️ No rows with valid dates found")
            return pd.DataFrame()
        
        logger.info(f"📊 Found {len(valid_dates_df)} rows with valid dates")
        
        # Filter to latest N days (up to yesterday)
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        logger.info(f"📅 Today: {today}, Yesterday: {yesterday}, Filtering dates <= {yesterday}")
        
        valid_dates_df = valid_dates_df[valid_dates_df['_parsed_date'] <= yesterday].copy()
        
        if len(valid_dates_df) == 0:
            logger.warning("⚠️ No rows with dates <= yesterday found")
            return pd.DataFrame()
        
        # Get unique dates and select latest N
        unique_dates = sorted(valid_dates_df['_parsed_date'].unique(), reverse=True)[:days_to_fetch]
        unique_dates = sorted(unique_dates)  # Sort chronologically for display
        
        if len(unique_dates) == 0:
            logger.warning("⚠️ No valid dates found for trend analysis")
            return pd.DataFrame()
        
        logger.info(f"✅ Selected {len(unique_dates)} latest dates: {[d.strftime('%d-%b-%Y') for d in unique_dates]}")
        
        # Filter to only selected dates
        valid_dates_df = valid_dates_df[valid_dates_df['_parsed_date'].isin(unique_dates)].copy()
        
        logger.info(f"📊 Filtered to {len(valid_dates_df)} rows for selected dates")
        
        # Helper function to extract numeric percentage values
        def extract_percentage(value):
            if pd.isna(value) or value == '' or value is None:
                return None
            if isinstance(value, (int, float)):
                return float(value) if not pd.isna(value) else None
            if isinstance(value, str):
                cleaned = str(value).replace('%', '').replace(',', '').replace(' ', '').strip()
                if cleaned:
                    try:
                        return float(cleaned)
                    except (ValueError, TypeError):
                        return None
            try:
                num_val = pd.to_numeric(value, errors='coerce')
                return float(num_val) if not pd.isna(num_val) else None
            except:
                return None
        
        # Extract Conv % values from the Conv % column
        valid_dates_df['_conv_value'] = valid_dates_df[conv_col_name].apply(extract_percentage)
        
        # Log sample data
        if len(valid_dates_df) > 0:
            sample_row = valid_dates_df.iloc[0]
            logger.info(f"📋 Sample row: Hub='{sample_row.get('Hub Name', 'N/A')}', Date='{sample_row.get(date_col_name, 'N/A')}', Conv %='{sample_row.get(conv_col_name, 'N/A')}', Parsed={sample_row.get('_conv_value', 'N/A')}")
        
        # Filter by target hub names
        if target_hub_names:
            logger.info(f"🔍 Filtering by {len(target_hub_names)} target hub names...")
            
            # Normalize hub names for comparison
            valid_dates_df['Hub Name'] = valid_dates_df['Hub Name'].astype(str).str.strip()
            normalized_target_hubs = [str(h).strip() for h in target_hub_names]
            
            # Create mapping for case-insensitive matching
            target_hubs_lower = {h.lower(): h for h in normalized_target_hubs}
            available_hubs_lower = {h.lower(): h for h in valid_dates_df['Hub Name'].dropna().unique()}
            
            # Find matching hubs
            matched_source_hubs = []
            for target_hub in normalized_target_hubs:
                target_lower = target_hub.lower()
                if target_lower in available_hubs_lower:
                    source_hub = available_hubs_lower[target_lower]
                    matched_source_hubs.append(source_hub)
                    if target_hub != source_hub:
                        logger.info(f"🔍 Matched '{target_hub}' to source hub '{source_hub}' (case variation)")
            
            if matched_source_hubs:
                valid_dates_df = valid_dates_df[valid_dates_df['Hub Name'].isin(matched_source_hubs)].copy()
                logger.info(f"✅ Filtered to {len(valid_dates_df)} rows for {len(matched_source_hubs)} target hubs")
            else:
                logger.warning("⚠️ No hubs found matching target hub names")
                return pd.DataFrame()
        
        # Filter by target hub names
        if target_hub_names:
            logger.info(f"🔍 Filtering by {len(target_hub_names)} target hub names...")
            
            # Normalize hub names for comparison
            valid_dates_df['Hub Name'] = valid_dates_df['Hub Name'].astype(str).str.strip()
            normalized_target_hubs = [str(h).strip() for h in target_hub_names]
            
            # Create mapping for case-insensitive matching
            target_hubs_lower = {h.lower(): h for h in normalized_target_hubs}
            available_hubs_lower = {h.lower(): h for h in valid_dates_df['Hub Name'].dropna().unique()}
            
            # Find matching hubs
            matched_source_hubs = []
            for target_hub in normalized_target_hubs:
                target_lower = target_hub.lower()
                if target_lower in available_hubs_lower:
                    source_hub = available_hubs_lower[target_lower]
                    matched_source_hubs.append(source_hub)
                    if target_hub != source_hub:
                        logger.info(f"🔍 Matched '{target_hub}' to source hub '{source_hub}' (case variation)")
            
            if matched_source_hubs:
                valid_dates_df = valid_dates_df[valid_dates_df['Hub Name'].isin(matched_source_hubs)].copy()
                logger.info(f"✅ Filtered to {len(valid_dates_df)} rows for {len(matched_source_hubs)} target hubs")
            else:
                logger.warning("⚠️ No hubs found matching target hub names")
                return pd.DataFrame()
        
        # Get unique hubs
        unique_hubs = sorted(valid_dates_df['Hub Name'].dropna().unique())
        logger.info(f"📊 Found {len(unique_hubs)} unique hubs")
        
        if len(unique_hubs) == 0:
            logger.warning("⚠️ No hubs found after filtering")
            return pd.DataFrame()
        
        # Format date column names for output
        formatted_date_cols = []
        for date_obj in unique_dates:
            formatted_date_cols.append(date_obj.strftime('%d-%b'))
        
        logger.info(f"📅 Creating trend table with dates as columns: {formatted_date_cols}")
        
        # Pivot the data: Create one row per hub, with dates as columns
        # Structure: Each row in source has Date, Hub_name, Conv % columns
        # We pivot so Hub Name becomes rows and dates become columns
        pivot_df = valid_dates_df.pivot_table(
            index='Hub Name',
            columns='_parsed_date',
            values='_conv_value',
            aggfunc='first'  # Use first value if duplicates exist
        ).reset_index()
        
        # Rename date columns to formatted strings
        new_columns = ['Hub Name']
        for col in pivot_df.columns[1:]:  # Skip 'Hub Name' column
            if isinstance(col, date):
                new_columns.append(col.strftime('%d-%b'))
            else:
                new_columns.append(str(col))
        
        pivot_df.columns = new_columns
        
        logger.info(f"📊 Pivoted data: {len(pivot_df)} hubs × {len(formatted_date_cols)} date columns")
        
        # Create Conversion % trend table (one row per Hub)
        conversion_data = []
        
        for hub_name in unique_hubs:
            hub_row = {'Hub Name': hub_name}
            
            # Add CLM Name and State from mapping (case-insensitive lookup via normalized dict)
            hub_name_normalized = str(hub_name).strip().lower() if hub_name else ""
            clm_state_match = _HUB_CLM_NORMALIZED_LOOKUP.get(hub_name_normalized) if hub_name_normalized else None
            
            if not clm_state_match and hub_name_normalized:
                logger.warning(f"⚠️ No CLM mapping for hub: '{hub_name}' (normalized: '{hub_name_normalized}')")
            
            if clm_state_match:
                hub_row['CLM Name'] = clm_state_match['CLM Name']
                hub_row['State'] = clm_state_match['State']
            else:
                hub_row['CLM Name'] = ''
                hub_row['State'] = ''
            
            # Add Volume Weight from mapping (case-insensitive lookup, fixed value with % sign, rounded to whole number)
            volume_weight_match = None
            for mapped_hub, weight in HUB_VOLUME_WEIGHT_MAPPING.items():
                if mapped_hub.lower() == hub_name_normalized:
                    volume_weight_match = weight
                    break
            
            if volume_weight_match is not None:
                hub_row['Volume Weight'] = f"{round(volume_weight_match)}%"
            else:
                hub_row['Volume Weight'] = ''
            
            # Extract Conv % values for each date from pivot_df
            hub_pivot_row = pivot_df[pivot_df['Hub Name'] == hub_name]
            
            if not hub_pivot_row.empty:
                for date_col in formatted_date_cols:
                    if date_col in hub_pivot_row.columns:
                        conv_val = hub_pivot_row[date_col].iloc[0]
                        if pd.notna(conv_val) and conv_val is not None:
                            hub_row[date_col] = f"{round(conv_val, 2)}%"
                        else:
                            hub_row[date_col] = "N/A"
                    else:
                        hub_row[date_col] = "N/A"
            else:
                # Hub not found in pivot - set all dates to N/A
                for date_col in formatted_date_cols:
                    hub_row[date_col] = "N/A"
            
            conversion_data.append(hub_row)
        
        # Create DataFrame
        conversion_df = pd.DataFrame(conversion_data)
        
        if conversion_df.empty:
            logger.warning("⚠️ No hub data found - DataFrame is empty")
            return pd.DataFrame()
        
        # Log extraction summary
        total_cells = len(unique_hubs) * len(formatted_date_cols)
        na_count = sum(1 for row in conversion_data for col in formatted_date_cols if row.get(col) == "N/A")
        extracted_count = total_cells - na_count
        logger.info(f"📊 Extraction summary: {extracted_count}/{total_cells} values extracted ({extracted_count*100/total_cells:.1f}%), {na_count} N/A values")
        
        # Calculate AVG column (average of last N days) for each hub
        avg_values = []
        for idx, row in conversion_df.iterrows():
            hub_name = row['Hub Name']
            # Extract percentage values from date columns
            date_values = []
            for date_col in formatted_date_cols:
                if date_col in row:
                    value_str = str(row[date_col])
                    # Extract numeric value from percentage string
                    if value_str and value_str != "N/A":
                        try:
                            # Remove % and convert to float
                            num_val = float(value_str.replace('%', '').strip())
                            date_values.append(num_val)
                        except (ValueError, AttributeError):
                            pass
            
            if len(date_values) > 0:
                avg_conversion = sum(date_values) / len(date_values)
                avg_values.append(f"{round(avg_conversion, 2)}%")
            else:
                avg_values.append("N/A")
        
        conversion_df['AVG'] = avg_values
        
        # Reorder columns: Hub Name, CLM Name, State, date columns, AVG, then Volume Weight
        cols = ['Hub Name', 'CLM Name', 'State'] + formatted_date_cols + ['AVG', 'Volume Weight']
        # Filter to only include columns that exist in the DataFrame
        cols = [col for col in cols if col in conversion_df.columns]
        # Create new DataFrame with only the specified columns in the correct order
        conversion_df = conversion_df[cols].copy()
        
        # Sort by AVG column in ascending order (before adding Total row)
        def extract_avg_value(avg_str):
            if pd.isna(avg_str) or avg_str == "N/A" or avg_str == '':
                return float('inf')  # Put N/A values at the end
            try:
                return float(str(avg_str).replace('%', '').strip())
            except (ValueError, AttributeError):
                return float('inf')  # Put invalid values at the end
        
        conversion_df['_sort_key'] = conversion_df['AVG'].apply(extract_avg_value)
        conversion_df = conversion_df.sort_values('_sort_key', ascending=True).reset_index(drop=True)
        conversion_df = conversion_df.drop(columns=['_sort_key'])  # Remove temporary sort key column
        
        logger.info("✅ Sorted hubs by AVG in ascending order")
        
        # Add Total row (weighted average across all hubs for each date using Volume Weight)
        total_row = {}
        total_row['Hub Name'] = 'Total'
        total_row['CLM Name'] = ''  # Empty for Total row
        total_row['State'] = ''  # Empty for Total row
        total_row['Volume Weight'] = ''  # Empty for Total row
        
        # Extract Volume Weight values (convert from "X%" format to numeric)
        def extract_volume_weight(weight_str):
            if pd.isna(weight_str) or weight_str == '' or weight_str is None:
                return None
            try:
                cleaned = str(weight_str).replace('%', '').strip()
                if cleaned:
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
            return None
        
        volume_weights = conversion_df['Volume Weight'].apply(extract_volume_weight).values
        
        # Calculate weighted average for each date column
        total_avg_values = []
        
        for date_col in formatted_date_cols:
            # Extract conversion values for this date column
            conversion_values = []
            weights_for_date = []
            
            # Use enumerate to get the position (not DataFrame index) for volume_weights array
            for position, (idx, row) in enumerate(conversion_df.iterrows()):
                date_value_str = str(row[date_col]) if date_col in row else ''
                conv_val = extract_percentage(date_value_str)
                # Use position (0-based) instead of DataFrame index
                weight_val = volume_weights[position] if position < len(volume_weights) else None
                
                # Only include if both conversion value and weight are available
                if conv_val is not None and weight_val is not None:
                    conversion_values.append(conv_val)
                    weights_for_date.append(weight_val)
            
            # Calculate weighted average: sum(value * weight) / sum(weights)
            if len(conversion_values) > 0 and len(weights_for_date) > 0:
                weighted_sum = sum(cv * w for cv, w in zip(conversion_values, weights_for_date))
                total_weight = sum(weights_for_date)
                if total_weight > 0:
                    weighted_avg = weighted_sum / total_weight
                    total_row[date_col] = f"{round(weighted_avg, 2)}%"
                    total_avg_values.append(weighted_avg)  # Store for AVG calculation
                else:
                    total_row[date_col] = "N/A"
            else:
                total_row[date_col] = "N/A"
        
        # Calculate overall weighted average for Total row AVG column
        if len(total_avg_values) > 0:
            overall_avg = sum(total_avg_values) / len(total_avg_values)
            total_row['AVG'] = f"{round(overall_avg, 2)}%"
        else:
            total_row['AVG'] = "N/A"
        
        # Ensure total_row has all required columns
        required_cols = ['Hub Name', 'CLM Name', 'State'] + formatted_date_cols + ['AVG', 'Volume Weight']
        for col in required_cols:
            if col not in total_row:
                if col in ['CLM Name', 'State', 'Volume Weight']:
                    total_row[col] = ''  # Empty string for these columns
                else:
                    total_row[col] = "N/A"  # N/A for date columns and AVG if missing
        
        # Create Total row DataFrame with ALL columns from conversion_df (in correct order)
        cols = ['Hub Name', 'CLM Name', 'State'] + formatted_date_cols + ['AVG', 'Volume Weight']
        cols = [col for col in cols if col in conversion_df.columns]
        
        # Build a list with values in the same order as columns
        total_row_values = []
        for col in cols:
            total_row_values.append(total_row.get(col, "N/A" if col not in ['CLM Name', 'State', 'Volume Weight'] else ''))
        
        # Create DataFrame with same columns and order as conversion_df
        total_df_row = pd.DataFrame([total_row_values], columns=cols)

        # Add OFD and Del+RVP sum rows (values come directly from source columns)
        def find_metric_column(headers, candidates):
            if not headers:
                return None
            for header in headers:
                if not header:
                    continue
                header_str = str(header).strip()
                header_lower = header_str.lower()
                for candidate in candidates:
                    candidate_lower = candidate.lower()
                    if header_str == candidate or header_lower == candidate_lower:
                        return header_str
                    if header_lower.replace(' ', '') == candidate_lower.replace(' ', ''):
                        return header_str
            return None

        metric_headers = df.attrs.get('original_headers', None)
        ofd_col_name = find_metric_column(metric_headers or df.columns, ['OFD'])
        del_rvp_col_name = find_metric_column(metric_headers or df.columns, ['Del+RVP', 'Del + RVP'])

        def build_sum_row(label, metric_col_name):
            row = {'Hub Name': label, 'CLM Name': '', 'State': '', 'Volume Weight': ''}
            date_sums = []
            for date_obj in unique_dates:
                date_key = date_obj.strftime('%d-%b')
                if not metric_col_name:
                    row[date_key] = "N/A"
                    continue
                date_rows = valid_dates_df[valid_dates_df['_parsed_date'] == date_obj]
                metric_values = date_rows[metric_col_name].apply(extract_percentage)
                metric_values = [v for v in metric_values if v is not None]
                if metric_values:
                    total_val = sum(metric_values)
                    row[date_key] = f"{round(total_val, 2)}"
                    date_sums.append(total_val)
                else:
                    row[date_key] = "N/A"
            if date_sums:
                row['AVG'] = f"{round(sum(date_sums) / len(date_sums), 2)}"
            else:
                row['AVG'] = "N/A"
            return row

        ofd_row = build_sum_row('OFD', ofd_col_name)
        del_rvp_row = build_sum_row('Del+RVP', del_rvp_col_name)

        # Add Revenue row (computed from preserved RPO row during write)
        revenue_row = {'Hub Name': 'Revenue', 'CLM Name': '', 'State': '', 'Volume Weight': ''}
        for date_obj in unique_dates:
            date_key = date_obj.strftime('%d-%b')
            revenue_row[date_key] = "N/A"
        revenue_row['AVG'] = "N/A"

        # Create DataFrames with same columns and order as conversion_df
        ofd_df_row = pd.DataFrame([[ofd_row.get(col, "N/A" if col not in ['CLM Name', 'State', 'Volume Weight'] else '') for col in cols]], columns=cols)
        del_rvp_df_row = pd.DataFrame([[del_rvp_row.get(col, "N/A" if col not in ['CLM Name', 'State', 'Volume Weight'] else '') for col in cols]], columns=cols)
        revenue_df_row = pd.DataFrame([[revenue_row.get(col, "N/A" if col not in ['CLM Name', 'State', 'Volume Weight'] else '') for col in cols]], columns=cols)

        # Append Total, OFD, Del+RVP, and Revenue rows
        logger.info(f"📊 Adding Total/OFD/Del+RVP/Revenue rows. DataFrame has {len(conversion_df)} rows before adding")
        conversion_df = pd.concat([conversion_df, total_df_row, ofd_df_row, del_rvp_df_row, revenue_df_row], ignore_index=True)
        logger.info(f"📊 DataFrame has {len(conversion_df)} rows after concatenation")
        
        # Verify Total row was added
        total_rows_count = len(conversion_df[conversion_df['Hub Name'] == 'Total'])
        logger.info(f"✅ Total row added: {total_rows_count} Total row(s) found in DataFrame")
        
        logger.info(f"✅ Created Conversion % trend table by dates: {len(conversion_df)} rows × {len(conversion_df.columns)} columns")
        
        # Generate summary of top 3 hubs dragging down Total Conversion
        generate_drag_down_summary(conversion_df, formatted_date_cols)
        
        return conversion_df
    
    except Exception as e:
        logger.error(f"❌ Error creating Conversion % trend table by dates: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        return pd.DataFrame()


def create_clm_wise_table(conversion_df, date_columns):
    """
    Create aggregated table grouped by CLM Name with weighted averages
    """
    try:
        # Exclude Total row from aggregation
        df_for_agg = conversion_df[~conversion_df['Hub Name'].isin(['Total', 'OFD', 'Del+RVP', 'Revenue'])].copy()
        
        if df_for_agg.empty:
            logger.warning("⚠️ No data to create CLM-wise table")
            return pd.DataFrame()
        
        # Helper functions
        def extract_percentage(value_str):
            if pd.isna(value_str) or value_str == '' or value_str is None:
                return None
            try:
                cleaned = str(value_str).replace('%', '').replace(',', '').strip()
                if cleaned and cleaned != "N/A":
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
            return None
        
        def extract_volume_weight(weight_str):
            if pd.isna(weight_str) or weight_str == '' or weight_str is None:
                return None
            try:
                cleaned = str(weight_str).replace('%', '').strip()
                if cleaned:
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
            return None
        
        # Get unique CLM Names
        unique_clms = sorted(df_for_agg['CLM Name'].dropna().unique())
        
        clm_data = []
        
        for clm_name in unique_clms:
            clm_row = {'CLM Name': clm_name}
            
            # Get all hubs for this CLM
            clm_hubs = df_for_agg[df_for_agg['CLM Name'] == clm_name]
            
            # Calculate weighted average for each date column
            clm_avg_values = []
            
            for date_col in date_columns:
                conversion_values = []
                weights_for_date = []
                
                for _, hub_row in clm_hubs.iterrows():
                    date_value_str = str(hub_row[date_col]) if date_col in hub_row else ''
                    conversion_val = extract_percentage(date_value_str)
                    weight_str = hub_row.get('Volume Weight', '')
                    weight_val = extract_volume_weight(weight_str)
                    
                    if conversion_val is not None and weight_val is not None:
                        conversion_values.append(conversion_val)
                        weights_for_date.append(weight_val)
                
                if len(conversion_values) > 0 and len(weights_for_date) > 0:
                    weighted_sum = sum(cv * w for cv, w in zip(conversion_values, weights_for_date))
                    total_weight = sum(weights_for_date)
                    if total_weight > 0:
                        weighted_avg = weighted_sum / total_weight
                        clm_row[date_col] = f"{round(weighted_avg, 2)}%"
                        clm_avg_values.append(weighted_avg)
                    else:
                        clm_row[date_col] = "N/A"
                else:
                    clm_row[date_col] = "N/A"
            
            # Calculate overall AVG
            if len(clm_avg_values) > 0:
                overall_avg = sum(clm_avg_values) / len(clm_avg_values)
                clm_row['AVG'] = f"{round(overall_avg, 2)}%"
            else:
                clm_row['AVG'] = "N/A"
            
            clm_data.append(clm_row)
        
        # Create DataFrame
        clm_df = pd.DataFrame(clm_data)
        
        # Ensure column order
        cols = ['CLM Name'] + date_columns + ['AVG']
        cols = [col for col in cols if col in clm_df.columns]
        clm_df = clm_df[cols].copy()
        
        # Add Total row (weighted average across all CLMs)
        total_row = {'CLM Name': 'Total'}
        total_avg_values = []
        
        for date_col in date_columns:
            conversion_values = []
            weights_for_date = []
            
            for _, clm_row in clm_df.iterrows():
                date_value_str = str(clm_row[date_col]) if date_col in clm_row else ''
                conversion_val = extract_percentage(date_value_str)
                
                # For CLM-wise Total, we need to get the total volume weight for each CLM
                # We'll use the hubs from the original conversion_df to calculate weights
                clm_name = clm_row['CLM Name']
                clm_hubs = df_for_agg[df_for_agg['CLM Name'] == clm_name]
                
                # Calculate total weight for this CLM
                clm_total_weight = 0
                for _, hub_row in clm_hubs.iterrows():
                    weight_str = hub_row.get('Volume Weight', '')
                    weight_val = extract_volume_weight(weight_str)
                    if weight_val is not None:
                        clm_total_weight += weight_val
                
                if conversion_val is not None and clm_total_weight > 0:
                    conversion_values.append(conversion_val)
                    weights_for_date.append(clm_total_weight)
            
            if len(conversion_values) > 0 and len(weights_for_date) > 0:
                weighted_sum = sum(cv * w for cv, w in zip(conversion_values, weights_for_date))
                total_weight = sum(weights_for_date)
                if total_weight > 0:
                    weighted_avg = weighted_sum / total_weight
                    total_row[date_col] = f"{round(weighted_avg, 2)}%"
                    total_avg_values.append(weighted_avg)
                else:
                    total_row[date_col] = "N/A"
            else:
                total_row[date_col] = "N/A"
        
        # Calculate overall AVG for Total row
        if len(total_avg_values) > 0:
            overall_avg = sum(total_avg_values) / len(total_avg_values)
            total_row['AVG'] = f"{round(overall_avg, 2)}%"
        else:
            total_row['AVG'] = "N/A"
        
        # Create Total row DataFrame and append
        total_df_row = pd.DataFrame([total_row])
        clm_df = pd.concat([clm_df, total_df_row], ignore_index=True)
        
        # Ensure column order
        cols = ['CLM Name'] + date_columns + ['AVG']
        cols = [col for col in cols if col in clm_df.columns]
        clm_df = clm_df[cols].copy()
        
        logger.info(f"✅ Created CLM-wise table: {len(clm_df)} rows × {len(clm_df.columns)} columns")
        return clm_df
    
    except Exception as e:
        logger.error(f"❌ Error creating CLM-wise table: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        return pd.DataFrame()


def create_state_wise_table(conversion_df, date_columns):
    """
    Create aggregated table grouped by State with weighted averages
    """
    try:
        # Exclude Total row from aggregation
        df_for_agg = conversion_df[~conversion_df['Hub Name'].isin(['Total', 'OFD', 'Del+RVP', 'Revenue'])].copy()
        
        if df_for_agg.empty:
            logger.warning("⚠️ No data to create State-wise table")
            return pd.DataFrame()
        
        # Helper functions
        def extract_percentage(value_str):
            if pd.isna(value_str) or value_str == '' or value_str is None:
                return None
            try:
                cleaned = str(value_str).replace('%', '').replace(',', '').strip()
                if cleaned and cleaned != "N/A":
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
            return None
        
        def extract_volume_weight(weight_str):
            if pd.isna(weight_str) or weight_str == '' or weight_str is None:
                return None
            try:
                cleaned = str(weight_str).replace('%', '').strip()
                if cleaned:
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
            return None
        
        # Get unique States
        unique_states = sorted(df_for_agg['State'].dropna().unique())
        
        state_data = []
        
        for state in unique_states:
            state_row = {'State': state}
            
            # Get all hubs for this State
            state_hubs = df_for_agg[df_for_agg['State'] == state]
            
            # Calculate weighted average for each date column
            state_avg_values = []
            
            for date_col in date_columns:
                conversion_values = []
                weights_for_date = []
                
                for _, hub_row in state_hubs.iterrows():
                    date_value_str = str(hub_row[date_col]) if date_col in hub_row else ''
                    conversion_val = extract_percentage(date_value_str)
                    weight_str = hub_row.get('Volume Weight', '')
                    weight_val = extract_volume_weight(weight_str)
                    
                    if conversion_val is not None and weight_val is not None:
                        conversion_values.append(conversion_val)
                        weights_for_date.append(weight_val)
                
                if len(conversion_values) > 0 and len(weights_for_date) > 0:
                    weighted_sum = sum(cv * w for cv, w in zip(conversion_values, weights_for_date))
                    total_weight = sum(weights_for_date)
                    if total_weight > 0:
                        weighted_avg = weighted_sum / total_weight
                        state_row[date_col] = f"{round(weighted_avg, 2)}%"
                        state_avg_values.append(weighted_avg)
                    else:
                        state_row[date_col] = "N/A"
                else:
                    state_row[date_col] = "N/A"
            
            # Calculate overall AVG
            if len(state_avg_values) > 0:
                overall_avg = sum(state_avg_values) / len(state_avg_values)
                state_row['AVG'] = f"{round(overall_avg, 2)}%"
            else:
                state_row['AVG'] = "N/A"
            
            state_data.append(state_row)
        
        # Create DataFrame
        state_df = pd.DataFrame(state_data)
        
        # Ensure column order
        cols = ['State'] + date_columns + ['AVG']
        cols = [col for col in cols if col in state_df.columns]
        state_df = state_df[cols].copy()
        
        # Add Total row (weighted average across all States)
        total_row = {'State': 'Total'}
        total_avg_values = []
        
        for date_col in date_columns:
            conversion_values = []
            weights_for_date = []
            
            for _, state_row in state_df.iterrows():
                date_value_str = str(state_row[date_col]) if date_col in state_row else ''
                conversion_val = extract_percentage(date_value_str)
                
                # For State-wise Total, we need to get the total volume weight for each State
                # We'll use the hubs from the original conversion_df to calculate weights
                state_name = state_row['State']
                state_hubs = df_for_agg[df_for_agg['State'] == state_name]
                
                # Calculate total weight for this State
                state_total_weight = 0
                for _, hub_row in state_hubs.iterrows():
                    weight_str = hub_row.get('Volume Weight', '')
                    weight_val = extract_volume_weight(weight_str)
                    if weight_val is not None:
                        state_total_weight += weight_val
                
                if conversion_val is not None and state_total_weight > 0:
                    conversion_values.append(conversion_val)
                    weights_for_date.append(state_total_weight)
            
            if len(conversion_values) > 0 and len(weights_for_date) > 0:
                weighted_sum = sum(cv * w for cv, w in zip(conversion_values, weights_for_date))
                total_weight = sum(weights_for_date)
                if total_weight > 0:
                    weighted_avg = weighted_sum / total_weight
                    total_row[date_col] = f"{round(weighted_avg, 2)}%"
                    total_avg_values.append(weighted_avg)
                else:
                    total_row[date_col] = "N/A"
            else:
                total_row[date_col] = "N/A"
        
        # Calculate overall AVG for Total row
        if len(total_avg_values) > 0:
            overall_avg = sum(total_avg_values) / len(total_avg_values)
            total_row['AVG'] = f"{round(overall_avg, 2)}%"
        else:
            total_row['AVG'] = "N/A"
        
        # Create Total row DataFrame and append
        total_df_row = pd.DataFrame([total_row])
        state_df = pd.concat([state_df, total_df_row], ignore_index=True)
        
        # Ensure column order
        cols = ['State'] + date_columns + ['AVG']
        cols = [col for col in cols if col in state_df.columns]
        state_df = state_df[cols].copy()
        
        logger.info(f"✅ Created State-wise table: {len(state_df)} rows × {len(state_df.columns)} columns")
        return state_df
    
    except Exception as e:
        logger.error(f"❌ Error creating State-wise table: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        return pd.DataFrame()


def main():
    """Main function"""
    try:
        logger.info("="*60)
        logger.info("🚀 CONVERSION % TREND ANALYZER")
        logger.info("="*60)
        
        # Setup Google Sheets client
        client = get_google_sheets_client()
        
        # Read Conversion % source sheet
        logger.info("\n" + "="*60)
        logger.info("📊 EXTRACTING CONVERSION % TREND")
        logger.info("="*60)
        
        conversion_values, conversion_worksheet = read_conversion_source_sheet(client)
        
        if conversion_values is not None and len(conversion_values) > 0:
            # Convert to DataFrame
            conversion_df = convert_conversion_to_dataframe(conversion_values)
            
            if not conversion_df.empty:
                # Create Conversion % trend table by dates (filtered by target hub names, latest 7 days)
                conversion_trend_df = create_conversion_trend_table_by_dates(
                    conversion_df, 
                    target_hub_names=TARGET_HUB_NAMES,
                    days_to_fetch=CONVERSION_DAYS_TO_FETCH
                )
                
                if not conversion_trend_df.empty:
                    logger.info("⏳ Waiting 5 seconds before writing Conversion % trend...")
                    time.sleep(5)
                    
                    # Open destination spreadsheet for Conversion %
                    logger.info(f"📊 Opening Conversion % destination spreadsheet: {CONVERSION_DEST_SPREADSHEET_ID}")
                    conversion_dest_spreadsheet = client.open_by_key(CONVERSION_DEST_SPREADSHEET_ID)
                    logger.info(f"✅ Opened destination spreadsheet: {conversion_dest_spreadsheet.title}")
                    
                    # Get or create Conversion worksheet
                    try:
                        conversion_trend_worksheet = conversion_dest_spreadsheet.worksheet(CONVERSION_DEST_WORKSHEET_NAME)
                        logger.info(f"✅ Found worksheet '{CONVERSION_DEST_WORKSHEET_NAME}'")
                        time.sleep(1)
                    except gspread.WorksheetNotFound:
                        conversion_trend_worksheet = conversion_dest_spreadsheet.add_worksheet(
                            title=CONVERSION_DEST_WORKSHEET_NAME, rows=1000, cols=100
                        )
                        logger.info(f"✅ Created new worksheet '{CONVERSION_DEST_WORKSHEET_NAME}'")
                    
                    # Clear and write Conversion % trend table
                    # Preserve row 26 (RPO) and rows 28-40
                    output_start_row = 1
                    preserve_row = 26
                    max_output_end_row = 27  # Revenue row must be within row 27
                    num_cols_conv = len(conversion_trend_df.columns)

                    # Limit rows to fit within row 27, excluding preserved row 26
                    max_data_rows = (max_output_end_row - output_start_row + 1) - 1 - 1  # total rows - header - preserved row
                    if len(conversion_trend_df) > max_data_rows:
                        logger.warning(f"⚠️ Truncating Conversion % rows from {len(conversion_trend_df)} to {max_data_rows} to preserve row 26 and rows 28-40")
                        conversion_trend_df = conversion_trend_df.iloc[:max_data_rows].copy()

                    num_rows_conv = len(conversion_trend_df)
                    output_end_row = max_output_end_row

                    # Clear ranges excluding preserved row 26
                    clear_range(conversion_trend_worksheet, output_start_row, preserve_row - 1, num_cols_conv)
                    clear_range(conversion_trend_worksheet, preserve_row + 1, max_output_end_row, num_cols_conv)
                    
                    # Convert to serializable format
                    def convert_to_serializable_conv(obj):
                        if pd.isna(obj) or obj is None:
                            return None
                        elif isinstance(obj, (np.integer, np.int64, np.int32)):
                            return int(obj)
                        elif isinstance(obj, (np.floating, np.float64, np.float32)):
                            return float(obj)
                        elif isinstance(obj, np.bool_):
                            return bool(obj)
                        else:
                            return str(obj) if obj is not None else None
                    
                    df_serializable = conversion_trend_df.map(convert_to_serializable_conv)

                    # Write data in two parts to preserve row 26
                    skip_index = preserve_row - output_start_row - 1  # data index that would land on preserved row
                    first_part_df = df_serializable.iloc[:skip_index].copy()
                    second_part_df = df_serializable.iloc[skip_index:].copy()

                    set_with_dataframe(conversion_trend_worksheet, first_part_df, row=output_start_row, resize=False)
                    if not second_part_df.empty:
                        set_with_dataframe(
                            conversion_trend_worksheet,
                            second_part_df,
                            row=preserve_row + 1,
                            resize=False,
                            include_column_header=False
                        )
                    
                    num_rows_conv = len(conversion_trend_df)
                    num_cols_conv = len(conversion_trend_df.columns)
                    last_written_row = (preserve_row + len(second_part_df)) if not second_part_df.empty else (output_start_row + len(first_part_df))
                    
                    # Format the table
                    if num_cols_conv > 0:
                        if num_cols_conv <= 26:
                            last_col_letter = string.ascii_uppercase[num_cols_conv - 1]
                        else:
                            first_letter = string.ascii_uppercase[(num_cols_conv - 1) // 26 - 1]
                            second_letter = string.ascii_uppercase[(num_cols_conv - 1) % 26]
                            last_col_letter = f"{first_letter}{second_letter}"
                        
                        # Format header row
                        header_range = f'A{output_start_row}:{last_col_letter}{output_start_row}'
                        retry_api_call(conversion_trend_worksheet.format, header_range, {
                            'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
                            'textFormat': {'bold': True},
                            'horizontalAlignment': 'CENTER'
                        })
                        
                        # Format first three column headers (Hub Name, CLM Name, State) - left aligned
                        first_three_headers_range = f'A{output_start_row}:C{output_start_row}'
                        retry_api_call(conversion_trend_worksheet.format, first_three_headers_range, {
                            'horizontalAlignment': 'LEFT'
                        })
                        
                        def format_column_range(col_letter, fmt):
                            ranges = []
                            if last_written_row >= preserve_row:
                                if preserve_row - 1 >= output_start_row:
                                    ranges.append(f'{col_letter}{output_start_row}:{col_letter}{preserve_row - 1}')
                                if last_written_row >= preserve_row + 1:
                                    ranges.append(f'{col_letter}{preserve_row + 1}:{col_letter}{last_written_row}')
                            else:
                                ranges.append(f'{col_letter}{output_start_row}:{col_letter}{last_written_row}')
                            for r in ranges:
                                retry_api_call(conversion_trend_worksheet.format, r, fmt)

                        # Format first three columns (Hub Name, CLM Name, State)
                        format_column_range('A', {
                            'textFormat': {'bold': True},
                            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                        })
                        # Format CLM Name column
                        format_column_range('B', {
                            'textFormat': {'bold': True},
                            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                        })
                        # Format State column
                        format_column_range('C', {
                            'textFormat': {'bold': True},
                            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                        })
                        
                        # Format AVG column
                        if 'AVG' in conversion_trend_df.columns:
                            avg_col_idx = list(conversion_trend_df.columns).index('AVG')
                            if avg_col_idx < 26:
                                avg_col_letter = string.ascii_uppercase[avg_col_idx]
                            else:
                                first_letter = string.ascii_uppercase[(avg_col_idx) // 26 - 1]
                                second_letter = string.ascii_uppercase[(avg_col_idx) % 26]
                                avg_col_letter = f"{first_letter}{second_letter}"
                            
                            format_column_range(avg_col_letter, {
                                'textFormat': {'bold': True},
                                'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.7}
                            })
                        
                        # Format Volume Weight column
                        if 'Volume Weight' in conversion_trend_df.columns:
                            vol_weight_col_idx = list(conversion_trend_df.columns).index('Volume Weight')
                            if vol_weight_col_idx < 26:
                                vol_weight_col_letter = string.ascii_uppercase[vol_weight_col_idx]
                            else:
                                first_letter = string.ascii_uppercase[(vol_weight_col_idx) // 26 - 1]
                                second_letter = string.ascii_uppercase[(vol_weight_col_idx) % 26]
                                vol_weight_col_letter = f"{first_letter}{second_letter}"
                            
                            format_column_range(vol_weight_col_letter, {
                                'textFormat': {'bold': True},
                                'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.7}
                            })
                        
                        # Format summary rows if they exist
                        if num_rows_conv > 0:
                            summary_labels = ['Total', 'OFD', 'Del+RVP', 'Revenue']
                            for label in summary_labels:
                                if label in conversion_trend_df['Hub Name'].values:
                                    idx = conversion_trend_df.index[conversion_trend_df['Hub Name'] == label][0]
                                    row_num = output_start_row + 1 + idx
                                    if row_num >= preserve_row:
                                        row_num += 1  # skip preserved row 26
                                    summary_row_range = f'A{row_num}:{last_col_letter}{row_num}'
                                    retry_api_call(conversion_trend_worksheet.format, summary_row_range, {
                                        'textFormat': {'bold': True},
                                        'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.7}
                                    })
                    
                    logger.info(f"✅ Conversion % trend table: {len(conversion_trend_df)} rows × {len(conversion_trend_df.columns)} columns")
                    logger.info("✅ Applied formatting to 'Conversion' worksheet")
                    
                    # Get date columns from conversion_trend_df for aggregation tables
                    date_cols = [col for col in conversion_trend_df.columns if col not in ['Hub Name', 'CLM Name', 'State', 'AVG', 'Volume Weight']]

                    # Update Revenue row using preserved RPO row (row 26)
                    if 'Revenue' in conversion_trend_df['Hub Name'].values and 'Del+RVP' in conversion_trend_df['Hub Name'].values:
                        revenue_idx = conversion_trend_df.index[conversion_trend_df['Hub Name'] == 'Revenue'][0]
                        revenue_row_num = output_start_row + 1 + revenue_idx
                        if revenue_row_num >= preserve_row:
                            revenue_row_num += 1

                        del_rvp_row = conversion_trend_df[conversion_trend_df['Hub Name'] == 'Del+RVP'].iloc[0]
                        rpo_row_values = retry_api_call(conversion_trend_worksheet.row_values, preserve_row)

                        def parse_numeric(value_str):
                            if value_str is None:
                                return None
                            try:
                                cleaned = str(value_str).replace('%', '').replace(',', '').strip()
                                if cleaned and cleaned.upper() != "N/A":
                                    return float(cleaned)
                            except (ValueError, TypeError):
                                pass
                            return None

                        revenue_values = []
                        revenue_row_cells = []
                        for col_idx, col_name in enumerate(conversion_trend_df.columns):
                            if col_name == 'Hub Name':
                                revenue_row_cells.append('Revenue')
                            elif col_name in ['CLM Name', 'State', 'Volume Weight']:
                                revenue_row_cells.append('')
                            elif col_name == 'AVG':
                                # set after date columns
                                revenue_row_cells.append('N/A')
                            elif col_name in date_cols:
                                sheet_col_idx = col_idx + 1
                                rpo_val = parse_numeric(rpo_row_values[sheet_col_idx - 1] if sheet_col_idx - 1 < len(rpo_row_values) else None)
                                del_rvp_val = parse_numeric(del_rvp_row.get(col_name, None))
                                if rpo_val is not None and del_rvp_val is not None:
                                    revenue_val = rpo_val * del_rvp_val
                                    revenue_row_cells.append(f"{round(revenue_val, 2)}")
                                    revenue_values.append(revenue_val)
                                else:
                                    revenue_row_cells.append("N/A")
                            else:
                                revenue_row_cells.append('')

                        if revenue_values:
                            avg_val = round(sum(revenue_values) / len(revenue_values), 2)
                            if 'AVG' in conversion_trend_df.columns:
                                avg_idx = conversion_trend_df.columns.get_loc('AVG')
                                revenue_row_cells[avg_idx] = f"{avg_val}"

                        revenue_row_range = f'A{revenue_row_num}:{last_col_letter}{revenue_row_num}'
                        retry_api_call(conversion_trend_worksheet.update, revenue_row_range, [revenue_row_cells])
                    
                    # Send A1:S25 to WhatsApp
                    def _wh_log(msg, level):
                        if level == 'ERROR':
                            logger.error(msg)
                        elif level == 'WARNING':
                            logger.warning(msg)
                        else:
                            logger.info(msg)
                    try:
                        send_sheet_range_to_whatsapp(
                            conversion_trend_worksheet,
                            range="A1:S25",
                            caption=f"Conversion % Trend - {datetime.now().strftime('%d-%b-%Y %H:%M')}",
                            log_func=_wh_log,
                        )
                    except Exception as e:
                        logger.warning(f"WhatsApp send failed (non-fatal): {e}")
                    
                    # Calculate starting row for CLM table (Hub table rows + 2 blank rows)
                    hub_table_end_row = max_output_end_row
                    clm_table_start_row = 41  # preserve rows 28-40
                    
                    # Create CLM-wise table
                    logger.info("\n📊 Creating CLM-wise aggregated table...")
                    clm_wise_df = create_clm_wise_table(conversion_trend_df, date_cols)
                    
                    if not clm_wise_df.empty:
                        logger.info("⏳ Waiting 3 seconds before writing CLM-wise table...")
                        time.sleep(3)
                        
                        # Write CLM-wise table below hub table
                        num_rows_clm = len(clm_wise_df)
                        num_cols_clm = len(clm_wise_df.columns)
                        
                        df_serializable_clm = clm_wise_df.map(convert_to_serializable_conv)
                        set_with_dataframe(conversion_trend_worksheet, df_serializable_clm, row=clm_table_start_row, resize=False)
                        
                        # Format CLM-wise table
                        if num_cols_clm > 0:
                            if num_cols_clm <= 26:
                                last_col_letter_clm = string.ascii_uppercase[num_cols_clm - 1]
                            else:
                                first_letter = string.ascii_uppercase[(num_cols_clm - 1) // 26 - 1]
                                second_letter = string.ascii_uppercase[(num_cols_clm - 1) % 26]
                                last_col_letter_clm = f"{first_letter}{second_letter}"
                            
                            # Format header
                            header_range_clm = f'A{clm_table_start_row}:{last_col_letter_clm}{clm_table_start_row}'
                            retry_api_call(conversion_trend_worksheet.format, header_range_clm, {
                                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
                                'textFormat': {'bold': True},
                                'horizontalAlignment': 'CENTER'
                            })
                            
                            # Format CLM Name header to be left-aligned
                            clm_header_range = f'A{clm_table_start_row}:A{clm_table_start_row}'
                            retry_api_call(conversion_trend_worksheet.format, clm_header_range, {
                                'horizontalAlignment': 'LEFT'
                            })
                            
                            # Format CLM Name column
                            clm_col_range = f'A{clm_table_start_row}:A{clm_table_start_row + num_rows_clm}'
                            retry_api_call(conversion_trend_worksheet.format, clm_col_range, {
                                'textFormat': {'bold': True},
                                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                            })
                            
                            # Format Total row if it exists
                            if num_rows_clm > 0:
                                total_row_range_clm = f'A{clm_table_start_row + num_rows_clm}:{last_col_letter_clm}{clm_table_start_row + num_rows_clm}'
                                retry_api_call(conversion_trend_worksheet.format, total_row_range_clm, {
                                    'textFormat': {'bold': True},
                                    'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.7}
                                })
                        
                        logger.info(f"✅ CLM-wise table: {len(clm_wise_df)} rows × {len(clm_wise_df.columns)} columns")
                        
                        # Calculate starting row for State table (CLM table rows + 2 blank rows)
                        clm_table_end_row = clm_table_start_row + num_rows_clm
                        state_table_start_row = clm_table_end_row + 3  # 2 blank rows + 1 for header
                    else:
                        # If CLM table is empty, start State table after preserved rows
                        state_table_start_row = 41
                    
                    # Create State-wise table
                    logger.info("\n📊 Creating State-wise aggregated table...")
                    state_wise_df = create_state_wise_table(conversion_trend_df, date_cols)
                    
                    if not state_wise_df.empty:
                        logger.info("⏳ Waiting 3 seconds before writing State-wise table...")
                        time.sleep(3)
                        
                        # Write State-wise table below CLM table
                        num_rows_state = len(state_wise_df)
                        num_cols_state = len(state_wise_df.columns)
                        
                        df_serializable_state = state_wise_df.map(convert_to_serializable_conv)
                        set_with_dataframe(conversion_trend_worksheet, df_serializable_state, row=state_table_start_row, resize=False)
                        
                        # Format State-wise table
                        if num_cols_state > 0:
                            if num_cols_state <= 26:
                                last_col_letter_state = string.ascii_uppercase[num_cols_state - 1]
                            else:
                                first_letter = string.ascii_uppercase[(num_cols_state - 1) // 26 - 1]
                                second_letter = string.ascii_uppercase[(num_cols_state - 1) % 26]
                                last_col_letter_state = f"{first_letter}{second_letter}"
                            
                            # Format header
                            header_range_state = f'A{state_table_start_row}:{last_col_letter_state}{state_table_start_row}'
                            retry_api_call(conversion_trend_worksheet.format, header_range_state, {
                                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
                                'textFormat': {'bold': True},
                                'horizontalAlignment': 'CENTER'
                            })
                            
                            # Format State header to be left-aligned
                            state_header_range = f'A{state_table_start_row}:A{state_table_start_row}'
                            retry_api_call(conversion_trend_worksheet.format, state_header_range, {
                                'horizontalAlignment': 'LEFT'
                            })
                            
                            # Format State column
                            state_col_range = f'A{state_table_start_row}:A{state_table_start_row + num_rows_state}'
                            retry_api_call(conversion_trend_worksheet.format, state_col_range, {
                                'textFormat': {'bold': True},
                                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                            })
                            
                            # Format Total row if it exists
                            if num_rows_state > 0:
                                total_row_range_state = f'A{state_table_start_row + num_rows_state}:{last_col_letter_state}{state_table_start_row + num_rows_state}'
                                retry_api_call(conversion_trend_worksheet.format, total_row_range_state, {
                                    'textFormat': {'bold': True},
                                    'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.7}
                                })
                        
                        logger.info(f"✅ State-wise table: {len(state_wise_df)} rows × {len(state_wise_df.columns)} columns")
                    
                    # Generate spreadsheet URL
                    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{CONVERSION_DEST_SPREADSHEET_ID}/edit"
                    logger.info(f"\n🔗 Destination Sheet URL: {spreadsheet_url}")
                else:
                    logger.warning("⚠️ Conversion % trend table is empty")
            else:
                logger.warning("⚠️ Conversion % DataFrame is empty")
        else:
            logger.warning("⚠️ No data found in Conversion % source sheet")
        
        logger.info("\n" + "="*60)
        logger.info("✅ Successfully extracted Conversion % trend!")
        logger.info("="*60)
    
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise


if __name__ == "__main__":
    main()


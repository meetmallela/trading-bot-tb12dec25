"""
Organize Trading Bot Files - Move old/backup files to archive
Run this from: C:\\Users\\meetm\\OneDrive\\Desktop\\GCPPythonCode\\TGAPI\\TB_12Dec25
"""

import os
import shutil
from datetime import datetime

# Current directory - auto-detect instead of hardcoded path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")

# ACTIVE FILES - DO NOT ARCHIVE (currently in use)
ACTIVE_FILES = {
    # Core Production Files (actively running)
    'signal_parser_with_futures.py',              # Current parser
    'instrument_finder_df.py',                     # Required by parser
    'telegram_reader_production.py',               # Current telegram reader
    'order_placer_db_production.py',               # Current order placer
    'sl_monitor_with_trailing.py',                 # Current SL monitor
    
    # Database & Auth
    'database.py',                                  # Database utilities
    'auth_with_token_save.py',                     # Authentication
    
    # Utilities (potentially useful)
    'check_signals.py',                            # Signal checking
    'check_status.py',                             # Status checking
    'startup_check.py',                            # Startup validation
    'get_channel_ids.py',                          # Channel ID converter
    'generate_instruments_csv.py',                 # CSV generator
    'clean_database.py',                           # Database cleanup
}

# Files to DEFINITELY ARCHIVE (old backups, dated versions, test files)
ARCHIVE_FILES = {
    # Old dated backups of order_placer
    'order_placer_db_production_19dec25_1408.py',
    'order_placer_db_production_19dec25_1641.py',
    'order_placer_db_production_29dec25_1355IST.py',
    'order_placer_db_production_31dec25_1800.py',
    'order_placer_db_production_31dec25_v0.py',
    'order_placer_db_production_31decd2025_1912.py',
    'order_placer_db_production_backup_30dec25_1917.py',
    'order_placer_db_production_v0_19dec2025.py',
    'order_placer_db_production_v1_29dec25.py',
    'order_placer_db_enhanced.py',
    'order_placer_db_production_FUTURES.py',       # Superseded
    'order_placer_futures_patch.py',
    
    # Old dated backups of parser
    'signal_parser_with_claude_fallback_19dec25_1407.py',
    'signal_parser_with_claude_fallback_30dec25_1832IST.py',
    'signal_parser_with_claude_fallback_v0_18dec25.py',
    'signal_parser_with_claude_fallback_v0_19dec2025.py',
    'signal_parser_with_claude_fallback_v0_29dec25.py',
    'signal_parser_with_claude_fallback_v1_19dec2025.py',
    'signal_parser_with_futures_01Jan26_1344IST.py',
    'signal_parser_with_futures_AUTO_SL.py',       # Superseded by current
    'signal_parser_with_futures_COMMODITY_FIXED.py', # Superseded
    'signal_parser_enhanced_v2.py',
    'signal_parser_with_claude_fallback.py',
    'parser_expiry_wrapper.py',
    'futures_parser_addon.py',
    'minimal_futures_patch.py',
    
    # Old dated backups of SL monitor
    'sl_monitor_enhanced_19dec25_1411.py',
    'sl_monitor_enhanced_dontuse.py',
    'sl_monitor_enhanced_vo_19dec2025.py',
    'sl_monitor_with_trailing_02Jan26_0959IST.py',
    'sl_monitor_with_trailing_bkp_01jan26_1335IST.py',
    'sl_monitor_with_trailing_goodat1003am_29dec25.py',
    'sl_monitor_with_trailing_v0.py',
    
    # Old telegram reader versions
    'telegram_reader_production_FUTURES.py',
    'telegram_reader_production_FUTURES_v0.py',
    'telegram_reader_standalone.py',
    
    # Demo/Test files
    'demo_recursive_trailing.py',
    'demo_sl_calculation.py',
    'demo_trailing_sl.py',
    'test_signal_injector.py',
    
    # Old deployment guides
    'COMPLETE_DEPLOYMENT_GUIDE.py',
    'DEPLOYMENT_GUIDE.py',
    'FUTURES_DEPLOYMENT_GUIDE.py',
    
    # Old diagnostic/fix scripts
    'analyze_failed_messages.py',
    'check_crudeoil.py',
    'check_csv_columns.py',
    'check_jp_channel.py',
    'check_sensex_strikes.py',
    'check_valid_instruments.py',
    'compare_csv_files.py',
    'diagnose_csv.py',
    'diagnose_signal_311.py',
    'fix_all_signals.py',
    'fix_date_format.py',
    'fix_signals_311_315.py',
    'fix_signals_324_325.py',
    'inject_complete_signals.py',
    'update_parser_csv.py',
    'expiry_enrichment.py',
    'add_instrument_type_column.py',
    
    # Old CSV generators
    'create_instruments_master.py',
    'generate_instruments_csv_goldencopy_dont_Delete.py',
    'generate_instruments_csv_v1.py',
    'additional_symbols.py',
}

def create_archive_folder():
    """Create archive folder if it doesn't exist"""
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)
        print(f"Created archive folder: {ARCHIVE_DIR}")
    else:
        print(f"Archive folder exists: {ARCHIVE_DIR}")

def move_to_archive(filename):
    """Move a file to archive folder"""
    source = os.path.join(BASE_DIR, filename)
    dest = os.path.join(ARCHIVE_DIR, filename)
    
    if os.path.exists(source):
        try:
            shutil.move(source, dest)
            return True
        except Exception as e:
            print(f"  ERROR moving {filename}: {e}")
            return False
    else:
        print(f"  File not found: {filename}")
        return False

def main():
    print("="*80)
    print("TRADING BOT FILE ORGANIZER")
    print("="*80)
    print(f"Base Directory: {BASE_DIR}")
    print(f"Archive Directory: {ARCHIVE_DIR}")
    print("="*80)
    
    # Create archive folder
    create_archive_folder()
    
    print("\n" + "="*80)
    print("ACTIVE FILES (will NOT be moved):")
    print("="*80)
    for f in sorted(ACTIVE_FILES):
        if os.path.exists(os.path.join(BASE_DIR, f)):
            print(f"  OK {f}")
        else:
            print(f"  MISSING {f}")
    
    print("\n" + "="*80)
    print(f"ARCHIVING {len(ARCHIVE_FILES)} OLD FILES...")
    print("="*80)
    
    moved_count = 0
    failed_count = 0
    
    for filename in sorted(ARCHIVE_FILES):
        print(f"\nMoving: {filename}")
        if move_to_archive(filename):
            moved_count += 1
            print(f"  DONE")
        else:
            failed_count += 1
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Successfully moved: {moved_count} files")
    print(f"Failed/Not found: {failed_count} files")
    print(f"Archive location: {ARCHIVE_DIR}")
    
    print("\n" + "="*80)
    print("REMAINING ACTIVE FILES:")
    print("="*80)
    
    # List remaining .py files in base directory
    remaining = [f for f in os.listdir(BASE_DIR) 
                 if f.endswith('.py') and os.path.isfile(os.path.join(BASE_DIR, f))]
    
    for f in sorted(remaining):
        print(f"  {f}")
    
    print(f"\nTotal: {len(remaining)} Python files remaining in working directory")
    print("="*80)

if __name__ == '__main__':
    print("\nWARNING: This will move files to the archive folder!")
    print("Make sure you have backups before proceeding.\n")
    
    response = input("Do you want to proceed? (yes/no): ").strip().lower()
    
    if response == 'yes':
        main()
    else:
        print("\nCancelled. No files were moved.")

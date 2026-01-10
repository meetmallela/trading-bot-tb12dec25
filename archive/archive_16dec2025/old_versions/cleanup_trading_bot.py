"""
cleanup_trading_bot.py - Organize and archive trading bot files

This script will:
1. Create archive_16dec2025 folder
2. Move old/test/diagnostic files to archive
3. Keep only production files in main folder
4. Create organized structure
"""

import os
import shutil
from datetime import datetime
import json

# Define what to keep in production folder
PRODUCTION_FILES = {
    # Core bot files
    'telegram_reader_standalone.py',
    'order_placer_db_enhanced.py', 
    'sl_monitor_enhanced.py',
    'signal_parser_enhanced_v2.py',
    'signal_parser_with_claude_fallback.py',
    'database.py',
    'instrument_finder_df.py',
    
    # Configuration
    'parsing_rules_enhanced_v2.json',
    'kite_config.json',
    'auth_with_token_save.py',
    
    # Data files
    'trading.db',
    'instruments_cache.csv',
    'instruments_master.csv',
    'valid_instruments.csv',
    
    # Logs (keep recent)
    'telegram_reader.log',
    'order_placer.log',
    'sl_monitor.log',
    
    # Test tools
    'test_signal_injector.py',
    'create_instruments_master.py',
    
    # Session
    'anon.session',
}

# Categories for archiving
ARCHIVE_CATEGORIES = {
    'diagnostics': [
        'analyze_', 'check_', 'diagnose_', 'compare_', 'comprehensive_',
        'find_', 'replay_', 'reprocess_'
    ],
    'fixes': [
        'fix_', 'migrate_', 'retry_', 'bulk_fix_', 'update_'
    ],
    'old_versions': [
        '_v1.py', '_v2.py', '_v3.py', '_old.py', '_backup.py',
        'order_placer_db.py',  # Old version without enhanced
        'telegram_reader_enhanced.py',  # Old version
        'sl_monitor.py',  # Old version without enhanced
    ],
    'exports': [
        'signals_export_', 'signals_12', 'telegram_messages'
    ],
    'rules_old': [
        'parsing_rules.json',
        'parsing_rules_enhanced.json',
        'parsing_rules_enhanced_LATEST.json',
    ]
}


def create_folder_structure(base_path):
    """Create organized archive structure"""
    archive_path = os.path.join(base_path, 'archive_16dec2025')
    
    folders = [
        archive_path,
        os.path.join(archive_path, 'diagnostics'),
        os.path.join(archive_path, 'fixes'),
        os.path.join(archive_path, 'old_versions'),
        os.path.join(archive_path, 'exports'),
        os.path.join(archive_path, 'rules_old'),
        os.path.join(archive_path, 'logs_old'),
    ]
    
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        print(f"[OK] Created: {folder}")
    
    return archive_path


def categorize_file(filename):
    """Determine which category a file belongs to"""
    
    # Check if it's a production file
    if filename in PRODUCTION_FILES:
        return 'production'
    
    # Check categories
    for category, patterns in ARCHIVE_CATEGORIES.items():
        for pattern in patterns:
            if pattern in filename:
                return category
    
    # Default to old_versions if not recognized
    return 'old_versions'


def cleanup_folder(base_path):
    """Main cleanup function"""
    
    print("="*80)
    print("TRADING BOT CLEANUP - Starting")
    print("="*80)
    print(f"Base path: {base_path}\n")
    
    # Create archive structure
    archive_path = create_folder_structure(base_path)
    
    # Get all files
    all_files = []
    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)
        if os.path.isfile(item_path):
            all_files.append(item)
    
    print(f"\nFound {len(all_files)} files\n")
    
    # Categorize and move files
    stats = {
        'production': 0,
        'diagnostics': 0,
        'fixes': 0,
        'old_versions': 0,
        'exports': 0,
        'rules_old': 0,
        'logs_old': 0
    }
    
    production_files = []
    
    for filename in sorted(all_files):
        category = categorize_file(filename)
        
        if category == 'production':
            production_files.append(filename)
            stats['production'] += 1
        else:
            # Move to archive
            source = os.path.join(base_path, filename)
            dest_folder = os.path.join(archive_path, category)
            dest = os.path.join(dest_folder, filename)
            
            try:
                shutil.move(source, dest)
                print(f"[ARCHIVED] {category:15s} | {filename}")
                stats[category] += 1
            except Exception as e:
                print(f"[ERROR] Failed to move {filename}: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("CLEANUP SUMMARY")
    print("="*80)
    print(f"Production files (kept):     {stats['production']}")
    print(f"Diagnostic tools (archived): {stats['diagnostics']}")
    print(f"Fix scripts (archived):      {stats['fixes']}")
    print(f"Old versions (archived):     {stats['old_versions']}")
    print(f"Exports (archived):          {stats['exports']}")
    print(f"Old rules (archived):        {stats['rules_old']}")
    print("="*80)
    
    # Show production files
    print("\n" + "="*80)
    print("PRODUCTION FILES (Kept in main folder)")
    print("="*80)
    
    categories = {
        'Core Bot': ['telegram_reader', 'order_placer', 'sl_monitor', 'database.py'],
        'Parsers': ['signal_parser', 'instrument_finder'],
        'Config': ['parsing_rules', 'kite_config', 'auth_with_token'],
        'Data': ['trading.db', 'instruments_cache', 'instruments_master'],
        'Logs': ['.log'],
        'Tools': ['test_signal_injector', 'create_instruments_master']
    }
    
    for cat_name, patterns in categories.items():
        print(f"\n{cat_name}:")
        matched = []
        for pf in production_files:
            if any(p in pf for p in patterns):
                matched.append(pf)
        for f in matched:
            print(f"  ✓ {f}")
    
    # Create README in archive
    readme_path = os.path.join(archive_path, 'README.txt')
    with open(readme_path, 'w') as f:
        f.write(f"""Trading Bot Archive - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
================================================================================

This archive contains files moved during cleanup on December 16, 2025.

Structure:
- diagnostics/   : Diagnostic and analysis scripts
- fixes/         : One-time fix scripts for database/signals
- old_versions/  : Older versions of production scripts
- exports/       : Exported signal data and logs
- rules_old/     : Old parsing rules versions

Production folder contains only essential files needed to run the bot.

To restore a file, simply copy it back to the main folder.
""")
    
    print(f"\n[OK] Created archive README: {readme_path}")
    print("\n" + "="*80)
    print("✓ CLEANUP COMPLETE!")
    print("="*80)


if __name__ == "__main__":
    # Get current directory (assumes script is in the folder to clean)
    base_path = os.getcwd()
    
    # Confirm before proceeding
    print("\n" + "="*80)
    print("TRADING BOT CLEANUP TOOL")
    print("="*80)
    print(f"\nThis will organize files in: {base_path}")
    print("\nActions:")
    print("1. Create archive_16dec2025/ folder")
    print("2. Move old/test/diagnostic files to archive")
    print("3. Keep only production files in main folder")
    print("\nProduction files will be kept (see PRODUCTION_FILES in script)")
    
    response = input("\nProceed with cleanup? (yes/no): ")
    
    if response.lower() == 'yes':
        cleanup_folder(base_path)
    else:
        print("\n[CANCELLED] No changes made")

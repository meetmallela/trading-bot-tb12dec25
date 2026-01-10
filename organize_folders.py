"""
Organize Trading Bot Folders - Clean up old directories
Keeps: claudeagent, archive (current production folders)
Archives: Old folders and test directories
"""

import os
import shutil
from datetime import datetime

# Auto-detect current directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")

# Folders to KEEP (active/needed)
KEEP_FOLDERS = {
    'archive',                    # Current archive (just created)
    'claudeagent',               # Your agent files (keep as-is)
    '__pycache__',               # Python cache (auto-generated)
}

# Folders to ARCHIVE (old/unused)
ARCHIVE_FOLDERS = {
    '1',                         # Unknown test folder
    'archive_16dec2025',         # Old archive
    'config',                    # Old config folder
    'core',                      # Old core folder
    'files',                     # Old files folder
    'logs',                      # Old logs folder
    'mnt',                       # Old mount folder
    'parsers',                   # Old parsers folder
    'telegram_messages',         # Old telegram messages
    'trading',                   # Old trading folder
    'upload18dec2025',           # Old upload folder
    'utils',                     # Old utils folder
    '{config,core,parsers,trading,utils,dashboard',  # Malformed folder
    '{config,parsers,trading,core,utils,tests}',     # Malformed folder
}

# Files to ARCHIVE (old/large files)
ARCHIVE_FILES = {
    'files.zip',                 # Old zip
    'upload18dec2025.zip',       # Old zip
    'instruments_cache.csv',     # Duplicate (instruments_master.csv exists)
    'instruments_master.csv',    # Old (valid_instruments.csv is current)
    'trading_session.session',   # Old session
    'PARSER_FIX_GUIDE.md',       # Old guide
}

def move_to_archive(name, is_folder=False):
    """Move a file or folder to archive"""
    source = os.path.join(BASE_DIR, name)
    dest = os.path.join(ARCHIVE_DIR, name)
    
    if not os.path.exists(source):
        print(f"  Not found: {name}")
        return False
    
    try:
        if is_folder:
            shutil.move(source, dest)
        else:
            shutil.move(source, dest)
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False

def get_folder_size(folder_path):
    """Get total size of a folder in MB"""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(folder_path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
        return total_size / (1024 * 1024)  # Convert to MB
    except:
        return 0

def main():
    print("="*80)
    print("TRADING BOT FOLDER ORGANIZER")
    print("="*80)
    print(f"Base Directory: {BASE_DIR}")
    print(f"Archive Directory: {ARCHIVE_DIR}")
    print("="*80)
    
    # Ensure archive exists
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)
        print("Created archive folder")
    
    print("\n" + "="*80)
    print("FOLDERS TO KEEP (will NOT be moved):")
    print("="*80)
    for folder in sorted(KEEP_FOLDERS):
        folder_path = os.path.join(BASE_DIR, folder)
        if os.path.exists(folder_path):
            size = get_folder_size(folder_path)
            print(f"  OK {folder} ({size:.1f} MB)")
        else:
            print(f"  MISSING {folder}")
    
    print("\n" + "="*80)
    print(f"ARCHIVING {len(ARCHIVE_FOLDERS)} OLD FOLDERS...")
    print("="*80)
    
    folders_moved = 0
    folders_failed = 0
    
    for folder in sorted(ARCHIVE_FOLDERS):
        folder_path = os.path.join(BASE_DIR, folder)
        if os.path.exists(folder_path):
            size = get_folder_size(folder_path)
            print(f"\nMoving folder: {folder} ({size:.1f} MB)")
            if move_to_archive(folder, is_folder=True):
                folders_moved += 1
                print(f"  DONE")
            else:
                folders_failed += 1
        else:
            print(f"\nSkip (not found): {folder}")
    
    print("\n" + "="*80)
    print(f"ARCHIVING {len(ARCHIVE_FILES)} OLD FILES...")
    print("="*80)
    
    files_moved = 0
    files_failed = 0
    
    for filename in sorted(ARCHIVE_FILES):
        filepath = os.path.join(BASE_DIR, filename)
        if os.path.exists(filepath):
            size = os.path.getsize(filepath) / (1024 * 1024)  # MB
            print(f"\nMoving file: {filename} ({size:.1f} MB)")
            if move_to_archive(filename, is_folder=False):
                files_moved += 1
                print(f"  DONE")
            else:
                files_failed += 1
        else:
            print(f"\nSkip (not found): {filename}")
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Folders moved: {folders_moved}")
    print(f"Folders failed: {folders_failed}")
    print(f"Files moved: {files_moved}")
    print(f"Files failed: {files_failed}")
    print(f"\nArchive location: {ARCHIVE_DIR}")
    
    print("\n" + "="*80)
    print("REMAINING FOLDERS:")
    print("="*80)
    
    # List remaining folders
    folders = [f for f in os.listdir(BASE_DIR) 
               if os.path.isdir(os.path.join(BASE_DIR, f)) and not f.startswith('.')]
    
    for folder in sorted(folders):
        folder_path = os.path.join(BASE_DIR, folder)
        size = get_folder_size(folder_path)
        print(f"  {folder} ({size:.1f} MB)")
    
    print(f"\nTotal: {len(folders)} folders in working directory")
    
    print("\n" + "="*80)
    print("CURRENT FILES:")
    print("="*80)
    
    # List current files
    files = [f for f in os.listdir(BASE_DIR) 
             if os.path.isfile(os.path.join(BASE_DIR, f)) and not f.startswith('.')]
    
    for f in sorted(files):
        filepath = os.path.join(BASE_DIR, f)
        size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        if size > 1:
            print(f"  {f} ({size:.1f} MB)")
        else:
            size_kb = os.path.getsize(filepath) / 1024
            print(f"  {f} ({size_kb:.1f} KB)")
    
    print(f"\nTotal: {len(files)} files in working directory")
    print("="*80)

if __name__ == '__main__':
    print("\nWARNING: This will move folders and large files to archive!")
    print("Your 'claudeagent' folder will NOT be touched.")
    print("Make sure you have backups before proceeding.\n")
    
    response = input("Do you want to proceed? (yes/no): ").strip().lower()
    
    if response == 'yes':
        main()
    else:
        print("\nCancelled. No folders were moved.")

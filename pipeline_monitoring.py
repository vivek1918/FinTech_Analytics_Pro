"""
Simple monitoring for SQLite pipeline
"""

import sqlite3
import json
from datetime import datetime
import pandas as pd

def check_database_health(db_path):
    """Check database health"""
    print("🔍 Checking database health...")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get table info
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"📊 Found {len(tables)} tables")
        
        # Check each table
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  • {table}: {count:,} rows")
        
        # Check data freshness
        cursor.execute("SELECT MAX(transaction_date) FROM transactions")
        latest_transaction = cursor.fetchone()[0]
        
        if latest_transaction:
            print(f"📅 Latest transaction: {latest_transaction}")
        
        conn.close()
        print("✅ Database health check completed")
        
    except Exception as e:
        print(f"❌ Health check failed: {e}")

def backup_database(db_path, backup_dir="backups"):
    """Create a backup of the database"""
    import shutil
    import os
    
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{backup_dir}/fintech_backup_{timestamp}.db"
    
    try:
        shutil.copy2(db_path, backup_path)
        print(f"✅ Database backed up to: {backup_path}")
        return True
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        return False

if __name__ == "__main__":
    print("="*50)
    print("SQLite Database Monitor")
    print("="*50)
    
    # Load config
    try:
        with open('config_sqlite.json', 'r') as f:
            config = json.load(f)
        db_path = config['database']['path']
    except:
        db_path = "database/fintech_portfolio.db"
    
    print(f"Database: {db_path}")
    print("-" * 50)
    
    # Run checks
    check_database_health(db_path)
    print("-" * 50)
    
    # Ask for backup
    backup = input("Create backup? (y/n): ").lower()
    if backup == 'y':
        backup_database(db_path)
    
    print("="*50)
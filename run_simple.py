"""
Simple runner for Windows
"""

import subprocess
import sys
import os

def main():
    print("FinTech Data Engineering Project")
    print("=" * 50)
    
    while True:
        print("\nMenu:")
        print("1. Setup project (first time)")
        print("2. Run ETL pipeline")
        print("3. Launch dashboard")
        print("4. Check database")
        print("5. Exit")
        
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == "1":
            print("\nSetting up project...")
            os.system("python simple_setup.py")
        
        elif choice == "2":
            print("\nRunning ETL pipeline...")
            os.system("python simple_etl.py")
        
        elif choice == "3":
            print("\nLaunching dashboard...")
            print("Dashboard will open in your browser")
            print("Press Ctrl+C to stop")
            os.system("streamlit run simple_dashboard.py")
        
        elif choice == "4":
            print("\nChecking database...")
            import sqlite3
            try:
                conn = sqlite3.connect('database/fintech_portfolio.db')
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                print(f"Found {len(tables)} tables:")
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    count = cursor.fetchone()[0]
                    print(f"  {table[0]}: {count:,} rows")
                conn.close()
            except Exception as e:
                print(f"Error: {e}")
        
        elif choice == "5":
            print("\nGoodbye!")
            break
        
        else:
            print("Invalid choice")

if __name__ == "__main__":
    main()
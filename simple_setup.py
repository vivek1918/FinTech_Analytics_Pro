"""
Simple Setup Script for Windows
No emojis, no unicode issues
"""

import sqlite3
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

print("=" * 60)
print("FinTech Project - Simple Setup")
print("=" * 60)

# Create directories
for folder in ['data_sources', 'database', 'logs', 'backups']:
    os.makedirs(folder, exist_ok=True)
    print(f"Created folder: {folder}")

# 1. Setup SQLite database
print("\n1. Setting up SQLite database...")
db_path = "database/fintech_portfolio.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create tables
tables_sql = [
    # Customers table
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        joining_date TEXT,
        credit_score INTEGER,
        annual_income REAL,
        employment_status TEXT,
        residential_status TEXT,
        age INTEGER,
        state TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    
    # Loans table
    """
    CREATE TABLE IF NOT EXISTS loans (
        loan_id TEXT PRIMARY KEY,
        customer_id TEXT,
        disbursement_date TEXT,
        loan_amount REAL,
        interest_rate REAL,
        tenure_months INTEGER,
        loan_type TEXT,
        emi_amount REAL,
        current_status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    )
    """,
    
    # Transactions table
    """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id TEXT PRIMARY KEY,
        loan_id TEXT,
        transaction_date TEXT,
        amount REAL,
        payment_mode TEXT,
        status TEXT,
        bounce_flag INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (loan_id) REFERENCES loans(loan_id)
    )
    """,
    
    # Risk features table
    """
    CREATE TABLE IF NOT EXISTS risk_features (
        loan_id TEXT PRIMARY KEY,
        risk_score REAL,
        risk_grade TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (loan_id) REFERENCES loans(loan_id)
    )
    """,
    
    # Portfolio summary
    """
    CREATE TABLE IF NOT EXISTS portfolio_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        calculation_date TEXT,
        total_loans INTEGER,
        total_disbursed REAL,
        active_loans INTEGER,
        avg_interest_rate REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
]

for i, sql in enumerate(tables_sql):
    try:
        cursor.execute(sql)
        print(f"  Table {i+1} created successfully")
    except Exception as e:
        print(f"  Error creating table {i+1}: {e}")

conn.commit()
conn.close()
print("Database setup complete!")

# 2. Create sample data if your dataset doesn't exist
print("\n2. Checking for existing dataset...")

# Check if you have your own CSV files
csv_files = ['customers.csv', 'loans.csv', 'transactions.csv']
have_own_data = all(os.path.exists(f"data_sources/{file}") for file in csv_files)

if not have_own_data:
    print("Generating sample data...")
    
    # Create sample customers
    np.random.seed(42)
    n_customers = 1000
    customers = pd.DataFrame({
        'customer_id': [f'CUST{i:06d}' for i in range(1, n_customers+1)],
        'joining_date': pd.date_range('2022-01-01', periods=n_customers).strftime('%Y-%m-%d'),
        'credit_score': np.random.randint(300, 900, n_customers),
        'annual_income': np.random.uniform(300000, 5000000, n_customers),
        'employment_status': np.random.choice(['Employed', 'Self-Employed', 'Unemployed'], n_customers),
        'residential_status': np.random.choice(['Owned', 'Rented'], n_customers),
        'age': np.random.randint(22, 70, n_customers),
        'state': np.random.choice(['Maharashtra', 'Karnataka', 'Delhi', 'Tamil Nadu', 'Gujarat'], n_customers)
    })
    
    # Create sample loans
    n_loans = 5000
    loans = pd.DataFrame({
        'loan_id': [f'LN{i:06d}' for i in range(1, n_loans+1)],
        'customer_id': np.random.choice(customers['customer_id'], n_loans),
        'disbursement_date': pd.date_range('2023-01-01', periods=n_loans).strftime('%Y-%m-%d'),
        'loan_amount': np.random.choice([50000, 100000, 200000, 500000], n_loans),
        'interest_rate': np.random.uniform(8.0, 18.0, n_loans),
        'tenure_months': np.random.choice([12, 24, 36, 48], n_loans),
        'loan_type': np.random.choice(['Personal', 'Business', 'Home', 'Auto', 'Education'], n_loans),
        'emi_amount': np.random.uniform(5000, 50000, n_loans),
        'current_status': np.random.choice(['ACTIVE', 'CLOSED', 'DELINQUENT'], n_loans, p=[0.7, 0.25, 0.05])
    })
    
    # Create sample transactions
    n_transactions = 20000
    transactions = pd.DataFrame({
        'transaction_id': [f'TXN{i:06d}' for i in range(1, n_transactions+1)],
        'loan_id': np.random.choice(loans['loan_id'], n_transactions),
        'transaction_date': pd.date_range('2023-01-01', periods=n_transactions).strftime('%Y-%m-%d %H:%M:%S'),
        'amount': np.random.uniform(1000, 50000, n_transactions),
        'payment_mode': np.random.choice(['UPI', 'Net Banking', 'Debit Card', 'NEFT'], n_transactions),
        'status': np.random.choice(['SUCCESS', 'FAILED'], n_transactions, p=[0.9, 0.1]),
        'bounce_flag': np.random.choice([0, 1], n_transactions, p=[0.95, 0.05])
    })
    
    # Save to CSV
    customers.to_csv('data_sources/customers.csv', index=False)
    loans.to_csv('data_sources/loans.csv', index=False)
    transactions.to_csv('data_sources/transactions.csv', index=False)
    
    print(f"Created sample data: {n_customers} customers, {n_loans} loans, {n_transactions} transactions")
else:
    print("Using your existing CSV files")

# 3. Create simple configuration
print("\n3. Creating configuration...")
config = {
    "database": {
        "path": "database/fintech_portfolio.db"
    },
    "data_sources": {
        "customers": "data_sources/customers.csv",
        "loans": "data_sources/loans.csv",
        "transactions": "data_sources/transactions.csv"
    }
}

with open('config.json', 'w') as f:
    json.dump(config, f, indent=2)

print("\n" + "=" * 60)
print("SETUP COMPLETE!")
print("=" * 60)
print("\nNext steps:")
print("1. Run ETL: python simple_etl.py")
print("2. Launch dashboard: streamlit run simple_dashboard.py")
print("\nYour data should be in: data_sources/ folder")
print("Your database will be: database/fintech_portfolio.db")
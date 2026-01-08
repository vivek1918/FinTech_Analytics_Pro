"""
Simple ETL Pipeline for Windows
No emojis, no unicode issues
"""

import sqlite3
import pandas as pd
import numpy as np
import json
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SimpleETL:
    def __init__(self, config_file='config.json'):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.db_path = self.config['database']['path']
        self.conn = None
        self.connect_db()
    
    def connect_db(self):
        """Connect to SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def extract_data(self):
        """Extract data from CSV files"""
        logger.info("Extracting data from CSV files...")
        
        data = {}
        
        for key, file_path in self.config['data_sources'].items():
            try:
                df = pd.read_csv(file_path)
                data[key] = df
                logger.info(f"Loaded {len(df)} rows from {file_path}")
            except Exception as e:
                logger.error(f"Failed to load {file_path}: {e}")
                data[key] = pd.DataFrame()
        
        return data
    
    def transform_data(self, data):
        """Transform the data"""
        logger.info("Transforming data...")
        
        transformed = {}
        
        # 1. Transform customers
        if 'customers' in data and not data['customers'].empty:
            customers = data['customers'].copy()
            
            # Create customer segments
            conditions = [
                customers['credit_score'] >= 750,
                (customers['credit_score'] >= 700) & (customers['credit_score'] < 750),
                (customers['credit_score'] >= 650) & (customers['credit_score'] < 700),
                customers['credit_score'] < 650
            ]
            choices = ['Premium', 'Gold', 'Silver', 'Standard']
            customers['customer_segment'] = np.select(conditions, choices, default='Standard')
            
            transformed['customers'] = customers
        
        # 2. Transform loans
        if 'loans' in data and not data['loans'].empty:
            loans = data['loans'].copy()
            
            # Create risk bands
            loans['risk_band'] = pd.cut(
                loans['interest_rate'],
                bins=[0, 10, 12, 14, 16, 100],
                labels=['A', 'B', 'C', 'D', 'E']
            )
            
            # Calculate total payable
            loans['total_payable'] = loans['emi_amount'] * loans['tenure_months']
            loans['total_interest'] = loans['total_payable'] - loans['loan_amount']
            
            transformed['loans'] = loans
        
        # 3. Transform transactions
        if 'transactions' in data and not data['transactions'].empty:
            transactions = data['transactions'].copy()
            
            # Parse date
            if 'transaction_date' in transactions.columns:
                transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'])
                transactions['transaction_month'] = transactions['transaction_date'].dt.strftime('%Y-%m')
            
            transformed['transactions'] = transactions
        
        # 4. Create risk features
        if 'loans' in transformed and 'customers' in transformed:
            risk_features = self.create_risk_features(transformed['loans'], transformed['customers'])
            transformed['risk_features'] = risk_features
        
        logger.info("Data transformation completed")
        return transformed
    
    def create_risk_features(self, loans, customers):
        """Create risk features"""
        # Merge data
        merged = pd.merge(loans, customers, on='customer_id', how='left')
        
        risk_features = pd.DataFrame()
        risk_features['loan_id'] = merged['loan_id']
        
        # Calculate risk score (simplified)
        if 'credit_score' in merged.columns:
            risk_score = merged['credit_score'] * 0.1 + 20  # Simplified formula
        else:
            risk_score = np.random.uniform(50, 90, len(merged))
        
        risk_features['risk_score'] = np.clip(risk_score, 0, 100).round(2)
        
        # Create risk grade
        risk_features['risk_grade'] = pd.cut(
            risk_features['risk_score'],
            bins=[0, 60, 70, 80, 90, 100],
            labels=['E', 'D', 'C', 'B', 'A']
        )
        
        return risk_features
    
    def load_data(self, data):
        """Load data to database"""
        logger.info("Loading data to database...")
        
        try:
            # Load each dataframe
            for table_name, df in data.items():
                if not df.empty:
                    # Use to_sql to create/append table
                    df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                    logger.info(f"Loaded {len(df)} rows to {table_name}")
            
            # Create portfolio summary
            self.create_portfolio_summary()
            
            self.conn.commit()
            logger.info("Data loading completed successfully")
            
        except Exception as e:
            logger.error(f"Data loading failed: {e}")
            self.conn.rollback()
            raise
    
    def create_portfolio_summary(self):
        """Create portfolio summary"""
        try:
            # Get portfolio metrics
            query = """
            SELECT 
                date('now') as calculation_date,
                COUNT(*) as total_loans,
                SUM(loan_amount) as total_disbursed,
                SUM(CASE WHEN current_status = 'ACTIVE' THEN 1 ELSE 0 END) as active_loans,
                AVG(interest_rate) as avg_interest_rate
            FROM loans
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                insert_query = """
                INSERT INTO portfolio_summary 
                (calculation_date, total_loans, total_disbursed, active_loans, avg_interest_rate)
                VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(insert_query, result)
                logger.info("Portfolio summary created")
        
        except Exception as e:
            logger.error(f"Failed to create portfolio summary: {e}")
    
    def run_pipeline(self):
        """Run the complete ETL pipeline"""
        logger.info("Starting ETL pipeline...")
        start_time = datetime.now()
        
        try:
            # Extract
            raw_data = self.extract_data()
            
            # Transform
            transformed_data = self.transform_data(raw_data)
            
            # Load
            self.load_data(transformed_data)
            
            # Generate report
            self.generate_report(transformed_data)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info(f"ETL pipeline completed in {duration}")
            print(f"\n{'='*60}")
            print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
            print(f"{'='*60}")
            print(f"Duration: {duration}")
            print("\nData Summary:")
            for name, df in transformed_data.items():
                print(f"  {name}: {len(df):,} rows")
            print(f"{'='*60}")
            
            return True
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            print(f"\nETL pipeline failed: {e}")
            return False
    
    def generate_report(self, data):
        """Generate data quality report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {}
        }
        
        for name, df in data.items():
            report['summary'][name] = {
                'row_count': len(df),
                'column_count': len(df.columns),
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
            }
        
        with open('etl_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info("Report generated: etl_report.json")

if __name__ == "__main__":
    print("=" * 60)
    print("Simple ETL Pipeline")
    print("=" * 60)
    
    etl = SimpleETL()
    success = etl.run_pipeline()
    
    if etl.conn:
        etl.conn.close()
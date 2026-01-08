-- SQLite compatible schema
-- Note: SQLite doesn't support stored procedures or some advanced MySQL features

-- Customers Table
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    joining_date DATE NOT NULL,
    credit_score INTEGER,
    annual_income REAL,
    employment_status TEXT,
    residential_status TEXT,
    age INTEGER,
    state TEXT,
    customer_segment TEXT,
    customer_tenure_months INTEGER,
    income_band TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_customers_credit_score ON customers(credit_score);
CREATE INDEX IF NOT EXISTS idx_customers_income ON customers(annual_income);
CREATE INDEX IF NOT EXISTS idx_customers_state ON customers(state);

-- Loans Table
CREATE TABLE IF NOT EXISTS loans (
    loan_id TEXT PRIMARY KEY,
    customer_id TEXT,
    disbursement_date DATE NOT NULL,
    loan_amount REAL,
    interest_rate REAL,
    tenure_months INTEGER,
    loan_type TEXT,
    emi_amount REAL,
    current_status TEXT,
    risk_band TEXT,
    total_payable REAL,
    total_interest REAL,
    total_paid REAL DEFAULT 0,
    payment_count INTEGER DEFAULT 0,
    bounce_count INTEGER DEFAULT 0,
    paid_percentage REAL,
    bounce_rate REAL,
    is_delinquent BOOLEAN DEFAULT 0,
    dpd INTEGER DEFAULT 0,
    days_since_disbursement INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Indexes for loans
CREATE INDEX IF NOT EXISTS idx_loans_customer ON loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(current_status);
CREATE INDEX IF NOT EXISTS idx_loans_disbursement ON loans(disbursement_date);
CREATE INDEX IF NOT EXISTS idx_loans_risk_band ON loans(risk_band);

-- Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    loan_id TEXT,
    transaction_date TIMESTAMP NOT NULL,
    amount REAL,
    payment_mode TEXT,
    status TEXT,
    bounce_flag BOOLEAN DEFAULT 0,
    transaction_month TEXT,
    transaction_day_of_week TEXT,
    transaction_hour INTEGER,
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    amount_category TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (loan_id) REFERENCES loans(loan_id)
);

-- Indexes for transactions
CREATE INDEX IF NOT EXISTS idx_transactions_loan ON transactions(loan_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);

-- Risk Features Table
CREATE TABLE IF NOT EXISTS risk_features (
    loan_id TEXT PRIMARY KEY,
    utilization_risk TEXT,
    emi_to_income_ratio REAL,
    income_risk TEXT,
    combined_risk_score REAL,
    risk_grade TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (loan_id) REFERENCES loans(loan_id)
);

-- Portfolio Summary Table
CREATE TABLE IF NOT EXISTS portfolio_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    calculation_date DATE NOT NULL,
    total_loans INTEGER,
    total_disbursed REAL,
    active_loans INTEGER,
    delinquent_loans INTEGER,
    delinquency_rate REAL,
    avg_interest_rate REAL,
    weighted_avg_interest REAL,
    collection_efficiency REAL,
    bounce_rate REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Monthly Trends Table
CREATE TABLE IF NOT EXISTS monthly_trends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    month TEXT NOT NULL UNIQUE,
    collection_amount REAL,
    monthly_bounce_rate REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Views (SQLite supports views)
CREATE VIEW IF NOT EXISTS active_loan_portfolio AS
SELECT 
    l.loan_id,
    l.customer_id,
    l.loan_amount,
    l.interest_rate,
    l.current_status,
    l.paid_percentage,
    rf.combined_risk_score,
    rf.risk_grade
FROM loans l
LEFT JOIN risk_features rf ON l.loan_id = rf.loan_id
WHERE l.current_status = 'ACTIVE';

CREATE VIEW IF NOT EXISTS collection_efficiency_by_mode AS
SELECT 
    t.payment_mode,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN t.status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_transactions,
    SUM(CASE WHEN t.bounce_flag = 1 THEN 1 ELSE 0 END) as bounced_transactions,
    ROUND((SUM(CASE WHEN t.status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as success_rate,
    ROUND((SUM(CASE WHEN t.bounce_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as bounce_rate,
    SUM(t.amount) as total_amount
FROM transactions t
GROUP BY t.payment_mode;

-- Insert sample data for testing
INSERT OR IGNORE INTO customers (customer_id, joining_date, credit_score, annual_income, employment_status, age) 
VALUES ('TEST001', '2023-01-01', 750, 1000000, 'Employed', 30);
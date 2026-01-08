"""
SQLite-Compatible Advanced Queries
These will work with your SQLite database
"""

import sqlite3
import pandas as pd

def get_connection(db_path='database/fintech_portfolio.db'):
    """Get SQLite connection"""
    conn = sqlite3.connect(db_path)
    return conn

# ==================== ADVANCED ANALYTICAL QUERIES ====================

def cohort_analysis_sqlite():
    """
    Cohort Analysis: Customer Retention by Joining Quarter
    SQLite compatible version
    """
    query = """
    WITH customer_cohorts AS (
        SELECT 
            c.customer_id,
            strftime('%Y', c.joining_date) || '-Q' || 
            ((strftime('%m', c.joining_date) + 2) / 3) as cohort_quarter,
            MIN(l.disbursement_date) as first_loan_date
        FROM customers c
        JOIN loans l ON c.customer_id = l.customer_id
        GROUP BY c.customer_id, cohort_quarter
    ),
    loan_activity AS (
        SELECT 
            cc.customer_id,
            cc.cohort_quarter,
            strftime('%Y', l.disbursement_date) || '-Q' || 
            ((strftime('%m', l.disbursement_date) + 2) / 3) as loan_quarter,
            COUNT(DISTINCT l.loan_id) as loans_taken
        FROM customer_cohorts cc
        JOIN loans l ON cc.customer_id = l.customer_id
        GROUP BY cc.customer_id, cc.cohort_quarter, loan_quarter
    )
    SELECT 
        cohort_quarter,
        loan_quarter,
        COUNT(DISTINCT customer_id) as active_customers,
        SUM(loans_taken) as total_loans,
        ROUND(
            COUNT(DISTINCT customer_id) * 100.0 / 
            FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
                PARTITION BY cohort_quarter 
                ORDER BY loan_quarter
            ), 
            2
        ) as retention_rate
    FROM loan_activity
    GROUP BY cohort_quarter, loan_quarter
    ORDER BY cohort_quarter, loan_quarter;
    """
    
    return query

def risk_adjusted_return_sqlite():
    """
    Risk-Adjusted Return on Capital (RAROC)
    SQLite compatible version
    """
    query = """
    SELECT 
        l.risk_band,
        COUNT(*) as loan_count,
        SUM(l.loan_amount) as total_exposure,
        SUM(l.total_interest) as expected_interest,
        SUM(CASE 
            WHEN l.current_status = 'DEFAULT' 
            THEN l.loan_amount - COALESCE(l.total_paid, 0) 
            ELSE 0 
        END) as expected_loss,
        ROUND(
            (SUM(l.total_interest) - 
             SUM(CASE 
                 WHEN l.current_status = 'DEFAULT' 
                 THEN l.loan_amount - COALESCE(l.total_paid, 0) 
                 ELSE 0 
             END)) * 100.0 / 
            SUM(l.loan_amount), 
            2
        ) as raroc_percentage
    FROM loans l
    WHERE l.risk_band IS NOT NULL
    GROUP BY l.risk_band
    ORDER BY raroc_percentage DESC;
    """
    
    return query

def payment_behavior_analysis_sqlite():
    """
    Payment Behavior Analysis with Window Functions
    SQLite compatible version
    """
    query = """
    SELECT 
        t.customer_id,
        t.loan_id,
        t.transaction_date,
        t.amount,
        t.status,
        t.bounce_flag,
        AVG(t.amount) OVER (
            PARTITION BY t.customer_id 
            ORDER BY t.transaction_date 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) as moving_avg_payment,
        SUM(CASE WHEN t.status = 'SUCCESS' THEN 1 ELSE 0 END) OVER (
            PARTITION BY t.customer_id 
            ORDER BY t.transaction_date
        ) as cumulative_success_count,
        ROUND(
            SUM(CASE WHEN t.bounce_flag = 1 THEN 1 ELSE 0 END) OVER (
                PARTITION BY t.customer_id 
                ORDER BY t.transaction_date
            ) * 100.0 /
            COUNT(*) OVER (
                PARTITION BY t.customer_id 
                ORDER BY t.transaction_date
            ), 
            2
        ) as cumulative_bounce_rate
    FROM (
        SELECT 
            l.customer_id,
            t.*
        FROM transactions t
        JOIN loans l ON t.loan_id = l.loan_id
    ) t
    ORDER BY t.customer_id, t.transaction_date
    LIMIT 1000;  -- Limit results for demo
    """
    
    return query

def early_warning_system_sqlite():
    """
    Early Warning System for Potential Defaults
    SQLite compatible version
    """
    query = """
    WITH loan_metrics AS (
        SELECT 
            l.loan_id,
            l.customer_id,
            l.dpd,
            l.paid_percentage,
            l.bounce_rate,
            l.is_delinquent,
            rf.combined_risk_score,
            CASE 
                WHEN l.dpd > 30 THEN 'Stage 3 - High Risk'
                WHEN l.dpd BETWEEN 15 AND 30 THEN 'Stage 2 - Medium Risk'
                WHEN l.dpd BETWEEN 1 AND 15 THEN 'Stage 1 - Low Risk'
                ELSE 'Current'
            END as risk_stage,
            CASE 
                WHEN l.paid_percentage < (l.days_since_disbursement * 100.0 / (l.tenure_months * 30)) * 0.8 
                THEN 1 ELSE 0 
            END as behind_schedule
        FROM loans l
        LEFT JOIN risk_features rf ON l.loan_id = rf.loan_id
        WHERE l.current_status = 'ACTIVE'
    )
    SELECT 
        risk_stage,
        COUNT(*) as loan_count,
        SUM(CASE WHEN is_delinquent = 1 THEN 1 ELSE 0 END) as already_delinquent,
        SUM(CASE WHEN behind_schedule = 1 THEN 1 ELSE 0 END) as behind_schedule_count,
        ROUND(AVG(combined_risk_score), 2) as avg_risk_score,
        ROUND(AVG(bounce_rate), 2) as avg_bounce_rate
    FROM loan_metrics
    GROUP BY risk_stage
    ORDER BY 
        CASE risk_stage
            WHEN 'Stage 3 - High Risk' THEN 1
            WHEN 'Stage 2 - Medium Risk' THEN 2
            WHEN 'Stage 1 - Low Risk' THEN 3
            ELSE 4
        END;
    """
    
    return query

def customer_rfm_analysis_sqlite():
    """
    Customer Segmentation using RFM Analysis
    SQLite compatible version
    """
    query = """
    WITH customer_rfm AS (
        SELECT 
            c.customer_id,
            -- Recency: Days since last transaction
            COALESCE(
                JULIANDAY('now') - JULIANDAY(MAX(t.transaction_date)), 
                365
            ) as recency,
            -- Frequency: Number of successful transactions
            COALESCE(COUNT(DISTINCT t.transaction_id), 0) as frequency,
            -- Monetary: Total amount paid
            COALESCE(SUM(t.amount), 0) as monetary,
            -- Risk: Average risk score
            COALESCE(AVG(rf.combined_risk_score), 50) as avg_risk_score
        FROM customers c
        LEFT JOIN loans l ON c.customer_id = l.customer_id
        LEFT JOIN transactions t ON l.loan_id = t.loan_id AND t.status = 'SUCCESS'
        LEFT JOIN risk_features rf ON l.loan_id = rf.loan_id
        GROUP BY c.customer_id
    ),
    rfm_scores AS (
        SELECT 
            customer_id,
            recency,
            frequency,
            monetary,
            avg_risk_score,
            NTILE(4) OVER (ORDER BY recency DESC) as r_score,
            NTILE(4) OVER (ORDER BY frequency) as f_score,
            NTILE(4) OVER (ORDER BY monetary) as m_score,
            NTILE(4) OVER (ORDER BY avg_risk_score DESC) as risk_score
        FROM customer_rfm
    )
    SELECT 
        r_score || f_score || m_score as rfm_cell,
        CASE 
            WHEN r_score || f_score || m_score IN ('444', '443', '434') THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 2 THEN 'Loyal Customers'
            WHEN r_score >= 3 AND f_score BETWEEN 2 AND 3 THEN 'Potential Loyalists'
            WHEN r_score >= 3 AND f_score = 1 THEN 'New Customers'
            WHEN r_score = 2 THEN 'At Risk'
            WHEN r_score = 1 THEN 'Lost Customers'
            ELSE 'Others'
        END as segment,
        COUNT(*) as customer_count,
        ROUND(AVG(recency), 2) as avg_recency,
        ROUND(AVG(frequency), 2) as avg_frequency,
        ROUND(AVG(monetary), 2) as avg_monetary,
        ROUND(AVG(avg_risk_score), 2) as avg_risk_score
    FROM rfm_scores
    GROUP BY r_score, f_score, m_score
    ORDER BY r_score DESC, f_score DESC, m_score DESC;
    """
    
    return query

# ==================== SIMPLER QUERIES (Fallback) ====================

def portfolio_summary_simple():
    """Simple portfolio summary query"""
    query = """
    SELECT 
        'Total Loans' as metric, 
        COUNT(*) as value 
    FROM loans
    UNION ALL
    SELECT 
        'Total Exposure', 
        ROUND(SUM(loan_amount), 2)
    FROM loans
    UNION ALL
    SELECT 
        'Active Loans', 
        SUM(CASE WHEN current_status = 'ACTIVE' THEN 1 ELSE 0 END)
    FROM loans
    UNION ALL
    SELECT 
        'Delinquent Loans', 
        SUM(CASE WHEN current_status IN ('DELINQUENT', 'DEFAULT') THEN 1 ELSE 0 END)
    FROM loans
    UNION ALL
    SELECT 
        'Avg Interest Rate', 
        ROUND(AVG(interest_rate), 2)
    FROM loans
    UNION ALL
    SELECT 
        'Collection Rate', 
        ROUND(
            (SELECT SUM(amount) FROM transactions WHERE status = 'SUCCESS') * 100.0 / 
            (SELECT SUM(emi_amount * tenure_months) FROM loans), 
            2
        )
    FROM loans LIMIT 1;
    """
    
    return query

def loan_type_analysis_simple():
    """Simple loan type analysis"""
    query = """
    SELECT 
        loan_type,
        COUNT(*) as loan_count,
        ROUND(SUM(loan_amount), 2) as total_amount,
        ROUND(AVG(interest_rate), 2) as avg_interest_rate,
        ROUND(
            SUM(CASE WHEN current_status = 'ACTIVE' THEN loan_amount ELSE 0 END) * 100.0 / 
            SUM(loan_amount), 
            2
        ) as active_percentage
    FROM loans
    GROUP BY loan_type
    ORDER BY total_amount DESC;
    """
    
    return query

def customer_segmentation_simple():
    """Simple customer segmentation"""
    query = """
    SELECT 
        customer_segment,
        COUNT(*) as customer_count,
        ROUND(AVG(credit_score), 2) as avg_credit_score,
        ROUND(AVG(annual_income), 2) as avg_income,
        ROUND(AVG(age), 2) as avg_age
    FROM customers
    WHERE customer_segment IS NOT NULL
    GROUP BY customer_segment
    ORDER BY customer_count DESC;
    """
    
    return query

def payment_mode_analysis_simple():
    """Simple payment mode analysis"""
    query = """
    SELECT 
        payment_mode,
        COUNT(*) as transaction_count,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_count,
        SUM(CASE WHEN bounce_flag = 1 THEN 1 ELSE 0 END) as bounced_count,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / 
            COUNT(*), 
            2
        ) as success_rate,
        ROUND(
            SUM(CASE WHEN bounce_flag = 1 THEN 1 ELSE 0 END) * 100.0 / 
            COUNT(*), 
            2
        ) as bounce_rate
    FROM transactions
    GROUP BY payment_mode
    ORDER BY total_amount DESC;
    """
    
    return query

def risk_distribution_simple():
    """Simple risk distribution analysis"""
    query = """
    SELECT 
        rf.risk_grade,
        COUNT(*) as loan_count,
        ROUND(AVG(rf.combined_risk_score), 2) as avg_risk_score,
        ROUND(SUM(l.loan_amount), 2) as total_exposure,
        ROUND(AVG(l.interest_rate), 2) as avg_interest_rate,
        ROUND(
            SUM(CASE WHEN l.current_status IN ('DELINQUENT', 'DEFAULT') THEN 1 ELSE 0 END) * 100.0 / 
            COUNT(*), 
            2
        ) as default_rate
    FROM risk_features rf
    JOIN loans l ON rf.loan_id = l.loan_id
    WHERE rf.risk_grade IS NOT NULL
    GROUP BY rf.risk_grade
    ORDER BY rf.risk_grade;
    """
    
    return query

# ==================== EXECUTION FUNCTIONS ====================

def execute_query(query, db_path='database/fintech_portfolio.db'):
    """Execute SQL query and return results as DataFrame"""
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Query execution error: {e}")
        return pd.DataFrame()

def run_all_analytics():
    """Run all analytics queries and save results"""
    queries = {
        'portfolio_summary': portfolio_summary_simple(),
        'loan_type_analysis': loan_type_analysis_simple(),
        'customer_segmentation': customer_segmentation_simple(),
        'payment_mode_analysis': payment_mode_analysis_simple(),
        'risk_distribution': risk_distribution_simple(),
        'cohort_analysis': cohort_analysis_sqlite(),
        'risk_adjusted_return': risk_adjusted_return_sqlite(),
        'rfm_analysis': customer_rfm_analysis_sqlite()
    }
    
    results = {}
    
    print("Running Analytics Queries...")
    print("=" * 60)
    
    for name, query in queries.items():
        try:
            df = execute_query(query)
            if not df.empty:
                results[name] = df
                print(f"✓ {name}: {len(df)} rows")
                
                # Save to CSV
                df.to_csv(f'analytics_{name}.csv', index=False)
            else:
                print(f"✗ {name}: No results")
        except Exception as e:
            print(f"✗ {name} failed: {e}")
    
    print("=" * 60)
    print(f"Completed: {len(results)} queries successful")
    
    return results

if __name__ == "__main__":
    print("FinTech Analytics - SQLite Version")
    print("=" * 60)
    
    # Run all analytics
    results = run_all_analytics()
    
    # Display sample results
    if results:
        print("\nSample Results:")
        for name, df in list(results.items())[:3]:  # Show first 3
            print(f"\n{name.upper().replace('_', ' ')}:")
            print(df.head())
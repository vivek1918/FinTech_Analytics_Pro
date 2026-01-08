# FinTech Analytics Dashboard – Complete Solution

## 🚀 Overview
**FinTech Analytics Dashboard** is a comprehensive web-based analytics platform built with **Streamlit** that provides real-time financial data analysis, advanced SQL query capabilities, and rich interactive visualizations for FinTech companies, NBFCs, and analytics teams.
---

## 🎯 Live Demo
🔗 **Live Demo:** https://drive.google.com/file/d/1M5IzvqScWU3Z0l1pDqbl_N9lSrbczeXm/view?usp=sharing

---

## ✨ Key Features

### 📊 Dashboard Capabilities
- **Real-time Analytics** – Live monitoring of financial data  
- **Interactive Charts** – 30+ advanced visualization types  
- **Multi-tab Interface** – 6 specialized analytics sections  
- **AI-powered Insights** – Smart recommendations & simulations  
- **Responsive Design** – Optimized for desktop & mobile  

---

### 🔍 Analytics Modules
- **Portfolio Intelligence** – Loan distribution & exposure analysis  
- **Risk Analytics** – Risk scoring, correlation & heatmaps  
- **Customer Insights** – Segmentation & behavioral analysis  
- **Performance Metrics** – Revenue & transaction tracking  
- **Statistical Analysis** – Advanced descriptive statistics  
- **Predictive Insights** – Forecasting & scenario simulation  

---

### 💻 Technical Features
- **Advanced SQL Console** – Safe SQL execution & visualization  
- **Data Export** – CSV, Excel, JSON  
- **Database Admin** – Backup, restore & optimization  
- **Query History** – Save & reuse past queries  
- **Schema Explorer** – Browse database structure  

---

## 🛠️ Installation Guide

### ✅ Prerequisites
- Python **3.8+**
- pip
- Git
- Minimum **4GB RAM**

---

### 📦 Step-by-Step Installation

#### 1️⃣ Clone the Repository
```bash
git clone https://github.com/yourusername/fintech-analytics-dashboard.git
cd fintech-analytics-dashboard
```
---

## 📊 Sample SQL Queries

Below are some **advanced SQL queries** that can be directly executed inside the **FinTech Analytics Dashboard SQL Console** to generate deep financial insights.

---

### 1️⃣ Risk-Adjusted Return on Capital (RAROC) Analysis

This query calculates **RAROC** by risk band by considering:
- Total loan exposure  
- Expected interest income  
- Expected loss from defaulted loans  
- Payments already collected  

```sql
WITH loan_payments AS (
    SELECT
        loan_id,
        SUM(amount) AS total_paid
    FROM transactions
    WHERE status = 'SUCCESS'
    GROUP BY loan_id
)
SELECT
    l.risk_band,
    COUNT(*) AS loan_count,
    SUM(l.loan_amount) AS total_exposure,
    SUM(l.total_interest) AS expected_interest,
    SUM(
        CASE
            WHEN l.current_status = 'DEFAULT'
            THEN l.loan_amount - COALESCE(lp.total_paid, 0)
            ELSE 0
        END
    ) AS expected_loss,
    ROUND(
        (
            SUM(l.total_interest) -
            SUM(
                CASE
                    WHEN l.current_status = 'DEFAULT'
                    THEN l.loan_amount - COALESCE(lp.total_paid, 0)
                    ELSE 0
                END
            )
        ) / SUM(l.loan_amount) * 100,
        2
    ) AS raroc_percentage
FROM loans l
LEFT JOIN loan_payments lp
    ON l.loan_id = lp.loan_id
GROUP BY l.risk_band
ORDER BY raroc_percentage DESC
LIMIT 1000
```

---

### 2️⃣ Customer Segmentation & Borrowing Behavior Analysis

This query provides a segment-wise customer overview, including:
- Number of customers
- Average credit score
- Average income
- Total loans taken
- Total amount borrowed

  ```sql
  SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    AVG(c.credit_score) AS avg_credit_score,
    AVG(c.annual_income) AS avg_income,
    COUNT(DISTINCT l.loan_id) AS total_loans,
    SUM(l.loan_amount) AS total_borrowed
    FROM customers c
    LEFT JOIN loans l 
        ON c.customer_id = l.customer_id
    GROUP BY c.customer_segment
    ORDER BY customer_count DESC

@echo off
chcp 65001 > nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo ========================================
echo   FinTech Project Setup for Windows
echo ========================================
echo.

echo Step 1: Fixing Python package issues...
python -m pip install --upgrade pip setuptools wheel
python -m pip cache purge

echo Step 2: Installing minimal requirements...
python -m pip install pandas numpy sqlite3 streamlit plotly

echo Step 3: Creating project structure...
mkdir data_sources 2>nul
mkdir database 2>nul
mkdir logs 2>nul
mkdir backups 2>nul

echo Step 4: Creating database...
python simple_setup.py

echo.
echo ========================================
echo   SETUP COMPLETE!
echo ========================================
echo.
echo To run the dashboard:
echo   streamlit run simple_dashboard.py
echo.
pause
"""
Configuration for the ETL process.
"""

# Tickers to process
TICKERS = ['NVDA', 'QQQ']

# Date range for data download
START_DATE = '2019-01-01'
END_DATE = '2024-01-01'

# Database connection parameters
DB_HOST = "analisis-financiero-db.cli6emcs8771.us-east-2.rds.amazonaws.com"
DB_NAME = "analisis_financiero"
DB_USER = "postgres"
DB_PASSWORD = "12345678"
TABLE_NAME = "rendimientos_diarios"
SSL_MODE = "require"

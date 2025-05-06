import os
import requests
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error
from typing import List, Dict, Optional

# Load environment variables
load_dotenv()
API_KEY = os.getenv('API_KEY')

# Supabase connection details
SUPABASE_HOST = os.getenv('SUPABASE_HOST')
SUPABASE_DATABASE = os.getenv('SUPABASE_DATABASE', 'postgres')
SUPABASE_USER = os.getenv('SUPABASE_USER')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
SUPABASE_PORT = 6543
SUPABASE_URL = os.getenv('SUPABASE_URL')  # Direct connection URL (alternative to individual params)

# Constants
BASE_URL = "https://www.alphavantage.co/query"
FUNCTION = "INSIDER_TRANSACTIONS"
SYMBOLS = ["IBM", "AAPL", "MSFT", "GOOGL"]
CUTOFF_DAYS = 2 * 365  # 2 years

class ETLPipeline:
    def __init__(self):
        self.connection = None
        self.raw_data = []
        self.processed_data = []
        
    def extract(self) -> bool:
        """Fetch data from Alpha Vantage API for all symbols"""
        print("\n=== EXTRACTION PHASE ===")
        success = True
        
        for symbol in SYMBOLS:
            print(f"Fetching data for {symbol}...")
            data = self._fetch_api_data(symbol)
            
            if data:
                self.raw_data.append({
                    'symbol': symbol,
                    'data': data
                })
                print(f"Successfully fetched data for {symbol}")
            else:
                print(f"Failed to fetch data for {symbol}")
                success = False
            
            time.sleep(12)  # Respect API rate limits
        
        return success
    
    def transform(self) -> bool:
        """Process and clean the extracted data"""
        print("\n=== TRANSFORMATION PHASE ===")
        if not self.raw_data:
            print("No data to transform")
            return False
        
        cutoff_date = datetime.now() - timedelta(days=CUTOFF_DAYS)
        
        for symbol_data in self.raw_data:
            symbol = symbol_data['symbol']
            transactions = symbol_data['data'].get('data', [])
            
            for transaction in transactions:
                processed = self._process_transaction(transaction, symbol, cutoff_date)
                if processed:
                    self.processed_data.append(processed)
        
        print(f"Processed {len(self.processed_data)} total transactions")
        return len(self.processed_data) > 0
    
    def load(self) -> bool:
        """Load data into PostgreSQL database"""
        print("\n=== LOADING PHASE ===")
        if not self.processed_data:
            print("No data to load")
            return False
        
        try:
            self._establish_db_connection()
            self._create_table_if_not_exists()
            
            df = pd.DataFrame(self.processed_data)
            self._insert_data_to_db(df)
            self._save_to_csv(df)
            return True
            
        except Error as e:
            print(f"Database error: {e}")
            return False
        finally:
            self._close_db_connection()
    
    def _fetch_api_data(self, symbol: str) -> Optional[Dict]:
        """Fetch insider transactions for a given symbol"""
        params = {
            "function": FUNCTION,
            "symbol": symbol,
            "apikey": API_KEY
        }
        
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Error for {symbol}: {e}")
            return None
        
    def _safe_float(self, value, default=0.0):  # Add self as first parameter
        try:
            return float(value) if value else default
        except (ValueError, TypeError):
            return default
    
    def _process_transaction(self, transaction: Dict, symbol: str, cutoff_date: datetime) -> Optional[Dict]:
        if not transaction.get('transaction_date'):
            return None
            
        try:
            trans_date = datetime.strptime(transaction['transaction_date'], '%Y-%m-%d')
        except ValueError:
            return None
            
        if trans_date < cutoff_date:
            return None
        
        # Safe conversions with fallbacks
        shares = self._safe_float(transaction.get('shares'))
        price = self._safe_float(transaction.get('share_price'))
        
        return {
            'symbol': symbol,
            'date': transaction['transaction_date'],
            'executive': transaction.get('executive', ''),
            'title': transaction.get('executive_title', ''),
            'type': transaction.get('security_type', ''),
            'transaction': transaction.get('acquisition_or_disposal', ''),
            'shares': shares,
            'price': price
    }


    
    def _establish_db_connection(self):
        """Establish database connection to Supabase"""
        try:
            # If a direct connection URL is provided, use it (preferred method for Supabase)
            if SUPABASE_URL:
                self.connection = psycopg2.connect(SUPABASE_URL)
            else:
                # Otherwise use individual parameters
                self.connection = psycopg2.connect(
                    host=SUPABASE_HOST,
                    user=SUPABASE_USER,
                    password=SUPABASE_PASSWORD,
                    database=SUPABASE_DATABASE,
                    port=SUPABASE_PORT
                )
            self.connection.autocommit = False
            print("Supabase connection established")
        except Error as e:
            raise Error(f"Connection to Supabase failed: {e}")
    
    def _create_table_if_not_exists(self):
        """Create table if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS insider_transactions (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            executive VARCHAR(100),
            title VARCHAR(100),
            type VARCHAR(50),
            transaction VARCHAR(50),
            shares DECIMAL(10, 2),
            price DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
            UNIQUE (symbol, date, executive, shares, price)
        ) 
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(create_table_query)
            print("Table verified/created")
        except Error as e:
            raise Error(f"Table creation failed: {e}")
        finally:
            cursor.close()
    
    def _insert_data_to_db(self, df: pd.DataFrame):
        """Insert data into PostgreSQL with duplicate handling"""
        insert_query = """
        INSERT INTO insider_transactions 
        (symbol, date, executive, title, type, transaction, shares, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date, executive, shares, price) DO UPDATE SET 
            title = EXCLUDED.title,
            type = EXCLUDED.type,
            transaction = EXCLUDED.transaction
        """
        data = [(
            row['symbol'],
            row['date'],
            row['executive'],
            row['title'],
            row['type'],
            row['transaction'],
            row['shares'],
            row['price']
        ) for _, row in df.iterrows()]
        
        cursor = self.connection.cursor()
        try:
            cursor.executemany(insert_query, data)
            self.connection.commit()
            print(f"Successfully inserted/updated {cursor.rowcount} records")
        except Error as e:
            self.connection.rollback()
            raise Error(f"Insert failed: {e}")
        finally:
            cursor.close()
    
    def _save_to_csv(self, df: pd.DataFrame):
        """Save processed data to CSV"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        csv_filename = f'insider_transactions_{timestamp}.csv'
        df.to_csv(csv_filename, index=False)
        print(f"Data backup saved to {csv_filename}")
    
    def _close_db_connection(self):
        """Close database connection if open"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")

def main():
    """Execute the ETL pipeline"""
    pipeline = ETLPipeline()
    
    # Execute ETL steps
    if not pipeline.extract():
        print("Extraction phase failed")
        return
    
    if not pipeline.transform():
        print("Transformation phase produced no data")
        return
    
    if not pipeline.load():
        print("Loading phase failed")
        return
    
    print("\nETL pipeline completed successfully")

if __name__ == "__main__":
    main()

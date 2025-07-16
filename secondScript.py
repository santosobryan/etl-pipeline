import os
import requests
import pandas as pd
import time
import json
import dotenv
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error
from typing import Optional, Dict, List

# Load environment variables
load_dotenv()
API_KEY = os.getenv('SECOND_API')

# Constants
SUPABASE_HOST = os.getenv('SUPABASE_HOST')
SUPABASE_DATABASE = os.getenv('SUPABASE_DATABASE', 'postgres')
SUPABASE_USER = os.getenv('SUPABASE_USER')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
SUPABASE_PORT = int(os.getenv('SUPABASE_PORT', 6543))  # Use 6543 for connection pooling
SUPABASE_URL = os.getenv('SUPABASE_URL')  # Direct connection URL
BASE_URL = 'https://www.alphavantage.co/query'

class StockMarketETL:
    def __init__(self):
        self.connection = None
        self.raw_data = {}
        self.processed_data = {
            'gainers': [],
            'losers': [],
            'active': [],
            'metadata': {}
        }
        
    def extract(self) -> bool:
        """Fetch data from Alpha Vantage API"""
        print("\n=== EXTRACTION PHASE ===")
        data = self._fetch_api_data()
        
        if data:
            self.raw_data = data
            print("Data extraction successful")
            return True
        else:
            print("Data extraction failed")
            return False
    
    def transform(self) -> bool:
        """Process and clean the extracted data"""
        print("\n=== TRANSFORMATION PHASE ===")
        if not self.raw_data:
            print("No data to transform")
            return False
        
        try:
            # Process metadata
            self.processed_data['metadata'] = {
                'last_updated': self.raw_data.get('last_updated'),
                'description': self.raw_data.get('metadata')
            }
            
            # Define category mapping
            category_mapping = {
                'top_gainers': 'gainers',
                'top_losers': 'losers', 
                'most_actively_traded': 'active'
            }
            
            # Process each category
            for category in ['top_gainers', 'top_losers', 'most_actively_traded']:
                short_category = category_mapping[category]
                for item in self.raw_data.get(category, []):
                    processed = self._process_item(item, short_category)
                    if processed:
                        self.processed_data[short_category].append(processed)
            
            print(f"Processed {len(self.processed_data['gainers'])} gainers, "
                  f"{len(self.processed_data['losers'])} losers, "
                  f"{len(self.processed_data['active'])} active stocks")
            return True
            
        except Exception as e:
            print(f"Transformation error: {e}")
            return False
    
    def load(self) -> bool:
        """Load data into Supabase"""
        print("\n=== LOADING PHASE ===")
        if not self.processed_data:
            print("No data to load")
            return False
        
        try:
            self._establish_db_connection()
            self._create_tables_if_not_exists()
            
            # Load metadata
            self._load_metadata()
            
            # Load stock data for each category
            for category in ['gainers', 'losers', 'active']:
                df = pd.DataFrame(self.processed_data[category])
                self._insert_stock_data(df, category)
            
            self._save_to_csv()
            return True
            
        except Error as e:
            print(f"Database error: {e}")
            return False
        finally:
            self._close_db_connection()
    
    def _fetch_api_data(self) -> Optional[Dict]:
        """Fetch top gainers/losers/active data from API"""
        try:
            params = {
                'function': 'TOP_GAINERS_LOSERS',
                'apikey': API_KEY
            }
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Error: {e}")
            return None
    
    def _process_item(self, item: Dict, category: str) -> Optional[Dict]:
        """Process individual stock item"""
        try:
            return {
                'ticker': item.get('ticker', ''),
                'price': self._safe_float(item.get('price')),
                'change_amount': self._safe_float(item.get('change_amount')),
                'change_percentage': self._safe_float(item.get('change_percentage', '').replace('%', '')),
                'volume': self._safe_float(item.get('volume'), 0),
                'category': category,
                'last_updated': self.raw_data.get('last_updated')
            }
        except Exception as e:
            print(f"Error processing item {item}: {e}")
            return None
    
    def _safe_float(self, value, default=0.0):
        """Safely convert to float with fallback"""
        try:
            return float(value) if value else default
        except (ValueError, TypeError):
            return default
    
    def _establish_db_connection(self):
        """Establish database connection to Supabase"""
        try:
            # Use only the connection string from environment
            self.connection = psycopg2.connect(SUPABASE_URL)
            self.connection.autocommit = False
            print("Supabase connection established")
        except Error as e:
            print(f"Connection details used: {SUPABASE_URL}")  # Debug output
            raise Error(f"Connection to Supabase failed: {e}")
    
    def _create_tables_if_not_exists(self):
        """Create tables if they don't exist"""
        cursor = self.connection.cursor()
        
        try:
            # Metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_metadata (
                    id SERIAL PRIMARY KEY,
                    last_updated TIMESTAMPTZ NOT NULL,
                    description TEXT,
                    UNIQUE (last_updated)
                )
            """)
            
            # Stock data table (single table for all categories)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_movers (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    price DECIMAL(10, 4) NOT NULL,
                    change_amount DECIMAL(10, 4) NOT NULL,
                    change_percentage DECIMAL(10, 4) NOT NULL,
                    volume BIGINT NOT NULL,
                    category VARCHAR(10) NOT NULL,
                    last_updated TIMESTAMPTZ NOT NULL,
                    UNIQUE (ticker, category, last_updated)
                )
            """)
            
            print("Tables verified/created")
        except Error as e:
            raise Error(f"Table creation failed: {e}")
        finally:
            cursor.close()
    
    def _load_metadata(self):
        """Load metadata into database"""
        metadata = self.processed_data['metadata']
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO stock_metadata (last_updated, description)
                VALUES (%s, %s)
                ON CONFLICT (last_updated) DO UPDATE SET
                    description = EXCLUDED.description
            """, (
                metadata['last_updated'],
                metadata['description']
            ))
            
            self.connection.commit()
            print("Metadata loaded successfully")
        except Error as e:
            self.connection.rollback()
            raise Error(f"Metadata insert failed: {e}")
        finally:
            cursor.close()
    
    def _insert_stock_data(self, df: pd.DataFrame, category: str):
        """Insert stock data into database"""
        cursor = self.connection.cursor()
        
        try:
            # Delete old data for this category to prevent duplicates
            cursor.execute("""
                DELETE FROM stock_movers 
                WHERE category = %s
            """, (category,))
            
            # Insert new data
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stock_movers (
                        ticker, price, change_amount, 
                        change_percentage, volume, category, last_updated
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['ticker'],
                    row['price'],
                    row['change_amount'],
                    row['change_percentage'],
                    row['volume'],
                    row['category'],
                    row['last_updated']
                ))
            
            self.connection.commit()
            print(f"Successfully loaded {len(df)} {category} records")
        except Error as e:
            self.connection.rollback()
            raise Error(f"Stock data insert failed: {e}")
        finally:
            cursor.close()
    
    def _save_to_csv(self):
        """Save processed data to CSV files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        
        for category in ['gainers', 'losers', 'active']:
            df = pd.DataFrame(self.processed_data[category])
            filename = f'stock_{category}_{timestamp}.csv'
            df.to_csv(filename, index=False)
            print(f"Saved {category} data to {filename}")
    
    def _close_db_connection(self):
        """Close database connection if open"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def display_data(self):
        """Display the extracted data in a readable format"""
        if not self.raw_data:
            print("No data to display")
            return
        
        print(f"\nLast Updated: {self.raw_data.get('last_updated')}")
        print(f"Description: {self.raw_data.get('metadata')}")
        
        for category in ['top_gainers', 'top_losers', 'most_actively_traded']:
            print(f"\n{category.replace('_', ' ').title()}:")
            for item in self.raw_data.get(category, [])[:3]:  # Show first 3 items
                print(f"  {item.get('ticker')}: {item.get('price')} "
                      f"({item.get('change_percentage')})")

def main():
    """Execute the ETL pipeline"""
    pipeline = StockMarketETL()
    
    # Execute ETL steps
    if not pipeline.extract():
        print("Extraction phase failed")
        return
    
    pipeline.display_data()
    
    if not pipeline.transform():
        print("Transformation phase failed")
        return
    
    if not pipeline.load():
        print("Loading phase failed")
        return
    
    print("\nETL pipeline completed successfully")

if __name__ == "__main__":
    main()

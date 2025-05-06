# Insider Transactions ETL Pipeline 🚀

🔗 A Python ETL pipeline that extracts insider trading data from Alpha Vantage API, transforms it, and loads it into a Supabase database for use by the **Get Ahead Trading Hub** (a Next.js application project in another repository).

## 📋 Overview

This mini project:
1. **Extracts** insider trading data from Alpha Vantage API (for IBM, AAPL, MSFT, GOOGL)
2. **Transforms** the data by cleaning and filtering (last 2 years only)
3. **Loads** the processed data into Supabase PostgreSQL database
4. Creates a CSV backup of all processed data

## 🛠️ Technologies Used

- **Python** (ETL logic)
- **Alpha Vantage API** (data source)
- **Supabase** (PostgreSQL database)
- **psycopg2** (PostgreSQL adapter)
- **pandas** (data processing)
- **python-dotenv** (environment variables)

## ⚙️ Setup Instructions

### Prerequisites
- Python 3.8+
- Supabase account with PostgreSQL database
- Alpha Vantage API key

### Installation
1. Clone the repository
```bash
git clone https://github.com/yourusername/insider-transactions-etl.git
cd insider-transactions-etl
```
2. Create and activate virtual environment
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```
3. Install dependencies
```bash
pip install -r requirements.txt
```
4. Create .env file with your credentials
```
API_KEY=your_alpha_vantage_api_key
SUPABASE_HOST=your_supabase_host
SUPABASE_USER=your_supabase_user
SUPABASE_PASSWORD=your_supabase_password
SUPABASE_DATABASE=postgres
SUPABASE_PORT=5432
# OR use direct connection URL:
SUPABASE_URL=postgresql://user:password@host:port/database
```

## 🚀 Running the Pipeline

### Execute the main script
```bash
python main.py
```
The pipeline will:
1. Fetch data from Alpha Vantage (with rate limiting)
2. Process and clean the data
3. Load into Supabase database
4. Create a CSV backup

## 📊 Database Schema
### `insider_transactions` Table

```sql
CREATE TABLE insider_transactions (
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
```

## 📊 Expected Data Volume
- ~50-100 transactions per company per year
- 4 companies tracked
- 2 year retention → ~400-800 total records

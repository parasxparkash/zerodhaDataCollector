# Zerodha Market Data Collector

A comprehensive market data collection system for Indian stock markets using Zerodha's Kite Connect API. This system captures real-time market data including equities, Nifty/BankNifty indices, futures, and options, storing them in a TimescaleDB/PostgreSQL database with automated backup and holiday checking capabilities.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Database Schema](#database-schema)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Overview

This project is a market data collection system that:
- Connects to Zerodha's WebSocket API to receive real-time market data
- Collects data for equities, indices, futures, and options
- Stores data in TimescaleDB/PostgreSQL with optimized schema
- Handles trading holidays automatically
- Provides automated backup functionality
- Includes error handling and email notifications

## Features

- **Real-time Market Data Collection**: Captures live market data from Zerodha API
- **Multiple Instrument Types**: Supports equities, indices, futures, and options
- **Automatic Holiday Detection**: Checks for trading holidays to avoid unnecessary runs
- **Configurable Market Hours**: Set custom market open/close times
- **Data Deduplication**: Handles duplicate ticks with conflict resolution
- **Email Notifications**: Sends alerts for system status and errors
- **Automated Backups**: Daily backup of collected data
- **Comprehensive Logging**: Detailed logs for debugging and monitoring
- **Database Connection Pooling**: Efficient database management

## Prerequisites

- Python 3.7+
- PostgreSQL with TimescaleDB extension
- Zerodha Kite Connect API credentials
- Google Authenticator app for 2FA (if enabled)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/ZerodhaWebsocket.git
cd ZerodhaWebsocket
```

### 2. Install Dependencies

```bash
pip install -r market_data_requirements.txt
```

### 3. Set Up Database

1. Install PostgreSQL and TimescaleDB extension
2. Create the required databases:
   - `broker_tokens` - for storing access tokens
   - `market_data_equities` - for equity data
   - `market_data_options` - for options data
   - `market_data_banknifty_options` - for BankNifty options data

### 4. Install Additional Requirements

```bash
# Install ChromeDriver for Selenium (if not already installed)
# Or use webdriver-manager which handles this automatically
```

## Configuration

### 1. Update Configuration File

Edit `market_data_config.json` with your credentials:

```json
{
    "notification_recipients": "your_email@domain.com",
    "email_sender": "your_email_sender@gmail.com",
    "email_password": "your_app_password",
    "totp_secret": "your_totp_secret",
    "broker_username": "your_zerodha_id",
    "broker_password": "your_password",
    "api_key": "your_api_key",
    "api_secret": "your_api_secret",
    "database_host": "localhost",
    "database_user": "postgres",
    "database_password": "your_password",
    "database_port": 5432,
    "token_database_name": "broker_tokens",
    "equity_database_name": "market_data_equities",
    "options_database_name": "market_data_options",
    "banknifty_options_database_name": "market_data_banknifty_options",
    "market_close_hour": 15,
    "market_close_minute": 35,
    "backup_thread_count": 4
}
```

### 2. Configuration Parameters Explained

- `notification_recipients`: Email addresses to receive system notifications
- `email_sender`: Gmail address for sending notifications
- `email_password`: App password for Gmail (not regular password)
- `totp_secret`: TOTP secret for 2FA (found in Google Authenticator)
- `broker_username`: Your Zerodha trading ID
- `broker_password`: Your Zerodha password
- `api_key`: Your Zerodha API key
- `api_secret`: Your Zerodha API secret
- `database_host`: PostgreSQL host address
- `database_user`: PostgreSQL username
- `database_password`: PostgreSQL password
- `database_port`: PostgreSQL port (default 5432)
- `token_database_name`: Database name for storing tokens
- `equity_database_name`: Database name for equity data
- `options_database_name`: Database name for options data
- `banknifty_options_database_name`: Database name for BankNifty options
- `market_close_hour`: Market closing hour (24-hour format)
- `market_close_minute`: Market closing minute
- `backup_thread_count`: Number of threads for backup operations

## Usage

### 1. Main Execution Flow

The system runs through the following steps:

1. **Holiday Check**: Verifies if today is a trading holiday
2. **Configuration Validation**: Ensures configuration values are set
3. **Access Token Request**: Fetches or refreshes access token
4. **Equity Universe Update**: Updates the list of instruments to track
5. **Lookup Tables Creation**: Creates mapping tables for instruments
6. **Market Data Collection**: Runs the WebSocket ticker until market close
7. **Daily Backup**: Backs up collected data to archives

### 2. Running the System

Execute the main script:

```bash
python market_data_main.py
```

This will run the complete workflow from start to finish.

### 3. Running Individual Components

You can also run individual components for testing or specific tasks:

```bash
# Request broker access token manually
python broker_access_token_request.py

# Update equity universe
python equity_universe_updater.py

# Create instrument lookup tables
python instrument_lookup_tables_creator.py

# Run market data ticker standalone
python market_data_ticker.py

# Run daily backup
python daily_market_data_backup.py

# Check if today is a trading holiday
python check_trading_holiday.py

# Validate system configuration
python validate_system_config.py
```

### 4. Automated Execution

Set up a cron job to run the system automatically during market hours:

```bash
# Example cron job to run at 9 AM on weekdays
0 9 * * 1-5 /usr/bin/python3 /path/to/ZerodhaWebsocket/market_data_main.py
```

## Project Structure

```
ZerodhaWebsocket/
├── market_data_main.py          # Main entry point orchestrating the workflow
├── market_data_ticker.py        # WebSocket ticker for real-time data collection
├── broker_access_token_request.py # Fetches/refreshes access tokens
├── equity_universe_updater.py   # Updates instrument list from NSE
├── instrument_lookup_tables_creator.py # Creates instrument-to-table mappings
├── daily_market_data_backup.py # Daily data backup functionality
├── market_data_mailer.py        # Email notification system
├── check_trading_holiday.py     # Trading holiday detection
├── validate_system_config.py    # Configuration validation
├── system_error_logger.py       # Error logging system
├── market_data_config.json      # Main configuration file
├── market_data_requirements.txt # Python dependencies
├── ind_nifty500list.csv         # Nifty 500 stock list
├── tradingHolidaysAllYears.csv  # Trading holiday calendar
├── instrument_lookup_tables/    # Generated lookup tables
├── getExpiryPrefix/             # Utility for expiry calculations
│   ├── getExpiryPrefix.py
│   └── tradingHolidaysAllYears.csv
├── utils/                       # Utility modules
│   ├── config_manager.py        # Configuration management
│   ├── db_manager.py            # Database connection management
│   ├── error_handler.py         # Error handling utilities
│   └── logger.py                # Logging utilities
└── README.md                    # This file
```

## Database Schema

### Daily Data Table Structure

The system creates a unified table for all market data with the following structure:

```sql
CREATE TABLE daily_table (
    instrument_token BIGINT,
    tradingsymbol VARCHAR(100),
    tablename VARCHAR(100),
    dbname VARCHAR(100),
    timestamp TIMESTAMP,
    price DECIMAL(19,2),
    qty INTEGER,
    avgPrice DECIMAL(19,2),
    volume BIGINT,
    bQty INTEGER,
    sQty INTEGER,
    open DECIMAL(19,2),
    high DECIMAL(19,2),
    low DECIMAL(19,2),
    close DECIMAL(19,2),
    changeper DECIMAL(60,10),
    lastTradeTime TIMESTAMP,
    oi INTEGER,
    oiHigh INTEGER,
    oiLow INTEGER,
    bq0 INTEGER, bp0 DECIMAL(19,2), bo0 INTEGER,
    bq1 INTEGER, bp1 DECIMAL(19,2), bo1 INTEGER,
    bq2 INTEGER, bp2 DECIMAL(19,2), bo2 INTEGER,
    bq3 INTEGER, bp3 DECIMAL(19,2), bo3 INTEGER,
    bq4 INTEGER, bp4 DECIMAL(19,2), bo4 INTEGER,
    sq0 INTEGER, sp0 DECIMAL(19,2), so0 INTEGER,
    sq1 INTEGER, sp1 DECIMAL(19,2), so1 INTEGER,
    sq2 INTEGER, sp2 DECIMAL(19,2), so2 INTEGER,
    sq3 INTEGER, sp3 DECIMAL(19,2), so3 INTEGER,
    sq4 INTEGER, sp4 DECIMAL(19,2), so4 INTEGER,
    UNIQUE (instrument_token, timestamp)
);
```

### Key Features of the Schema

- **Conflict Resolution**: Uses `ON CONFLICT (instrument_token, timestamp) DO UPDATE` to handle duplicate ticks
- **Optimized Indexes**: Includes indexes on frequently queried columns
- **Market Depth**: Stores 5 levels of bid/ask data for each instrument
- **Unified Storage**: All instrument types stored in a single table with classification

### Sample Queries

```sql
-- Get latest price for NIFTY 50
SELECT timestamp, price FROM daily_table 
WHERE tradingsymbol='NIFTY 50' 
ORDER BY timestamp DESC LIMIT 5;

-- Get latest data for Nifty futures
SELECT timestamp, price, volume FROM daily_table 
WHERE tablename='NIFTYFUT' 
ORDER BY timestamp DESC LIMIT 5;

-- Get data for a specific instrument by token
SELECT timestamp, price, volume FROM daily_table 
WHERE instrument_token=13368834 
ORDER BY timestamp DESC LIMIT 5;
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Authentication Issues
- **Problem**: "Invalid API key" or "Access token expired"
- **Solution**: Verify API credentials in `market_data_config.json` and ensure the API key is enabled for WebSocket connections

#### 2. Database Connection Issues
- **Problem**: "Connection refused" or "Authentication failed"
- **Solution**: Check database credentials and ensure PostgreSQL is running with TimescaleDB extension installed

#### 3. ChromeDriver Issues
- **Problem**: "ChromeDriver not found" or "Browser automation failed"
- **Solution**: Ensure Chrome/Chromium is installed and webdriver-manager can download the appropriate driver

#### 4. Market Data Not Being Collected
- **Problem**: Empty database tables or no new entries
- **Solution**: 
  - Check if today is a trading holiday
  - Verify access token is valid
  - Ensure instrument tokens are correctly loaded
  - Review logs for errors

#### 5. Email Notifications Not Working
- **Problem**: No emails received for system events
- **Solution**: Verify email settings and ensure app password is used for Gmail

### Log Files

Check the log files in the `logs/` directory (created automatically) for detailed debugging information:
- `market_data_main.log` - Main workflow logs
- `market_data_ticker.log` - Ticker operation logs
- `error.log` - Error logs

### Debug Mode

For detailed debugging, you can run individual components with additional logging enabled.

## Maintenance

### Regular Tasks

1. **Database Maintenance**: Run VACUUM and ANALYZE regularly on large tables
2. **Log Rotation**: Implement log rotation to prevent disk space issues
3. **Backup Verification**: Regularly verify backup integrity
4. **Configuration Updates**: Update holiday calendar and instrument lists periodically

### Performance Optimization

- **Index Management**: Monitor and optimize indexes based on query patterns
- **Connection Pooling**: Adjust connection pool sizes based on system load
- **Batch Processing**: Tune batch sizes for optimal throughput
- **Memory Management**: Monitor memory usage during market hours

## Security Considerations

- Store sensitive credentials securely
- Use app passwords for email notifications instead of regular passwords
- Implement proper access controls for the database
- Regularly rotate API keys and other credentials
- Ensure network security for database connections

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please open an issue in the GitHub repository or contact the project maintainer.

---
*Author: Paras Parkash*
"""
Configuration module for the Air Quality Data Warehouse (DWH).

This module is responsible for loading environment variables from a local .env file
and exposing them as Python constants. It centralizes the configuration for both
the database connection and the OpenAQ API, ensuring that sensitive credentials 
(like passwords and API keys) are safely managed and not hardcoded in the scripts.
"""

import os

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# Database Configuration
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "master")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC+Driver+18+for+SQL+Server")

if not DB_PASSWORD:
    logger.warning("DB_PASSWORD is not set in the .env file!")

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

if not OPENAQ_API_KEY:
    logger.warning("OpenAQ API Key is not set in the .env file!")


if __name__ == "__main__":
    logger.info("Testing configuration module...")
    logger.info(f"Database Host: {DB_HOST}:{DB_PORT}")
    logger.info(f"Database User: {DB_USER}")
    
    is_password_loaded = "YES" if DB_PASSWORD else "NO"
    logger.info(f"Password loaded successfully: {is_password_loaded}")
from sqlalchemy import text
from loguru import logger
from src.db_tools import get_db_engine

# DDL (Data Definition Language) queries for our Star Schema
CREATE_TABLES_SQL = """
-- 1. Dimension: Date (Standard DWH dimension)
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Date' AND xtype='U')
CREATE TABLE Dim_Date (
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    WeekDayName VARCHAR(20) NOT NULL
);

-- 2. Dimension: Pollutant (Lookup table for measured parameters)
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Pollutant' AND xtype='U')
CREATE TABLE Dim_Pollutant (
    PollutantKey INT IDENTITY(1,1) PRIMARY KEY,
    ParameterName VARCHAR(50) NOT NULL UNIQUE, -- e.g., 'PM10', 'PM2.5'
    Unit VARCHAR(20) NOT NULL -- e.g., 'µg/m³'
);

-- 3. Dimension: Station (SCD Type 2 implementation)
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Station' AND xtype='U')
CREATE TABLE Dim_Station (
    StationKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    StationId INT NOT NULL,                   -- Natural Key from API
    StationName VARCHAR(255) NOT NULL,
    City VARCHAR(100),
    -- SCD Type 2 tracking columns
    ValidFrom DATETIME NOT NULL,
    ValidTo DATETIME NULL,
    IsActive BIT NOT NULL DEFAULT 1
);

-- 4. Fact: Air Quality (Central table storing measurements)
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Fact_AirQuality' AND xtype='U')
CREATE TABLE Fact_AirQuality (
    FactKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    DateKey INT NOT NULL FOREIGN KEY REFERENCES Dim_Date(DateKey),
    StationKey INT NOT NULL FOREIGN KEY REFERENCES Dim_Station(StationKey),
    PollutantKey INT NOT NULL FOREIGN KEY REFERENCES Dim_Pollutant(PollutantKey),
    MeasurementValue FLOAT NOT NULL,
    MeasurementTime DATETIME NOT NULL, -- Exact time of reading
    
    -- Ensuring we don't load exact duplicates
    CONSTRAINT UQ_AirQuality_Measurement UNIQUE (DateKey, StationKey, PollutantKey, MeasurementTime)
);
"""

def initialize_schema() -> None:
    """
    Connects to the database and executes the DDL scripts to create the Star Schema.
    """
    logger.info("Starting Data Warehouse schema initialization...")
    engine = get_db_engine()
    
    try:
        with engine.begin() as conn:
            conn.execute(text(CREATE_TABLES_SQL))
        logger.success("Star Schema tables created successfully (or already exist).")
    except Exception as e:
        logger.error(f"Failed to initialize schema: {e}")
        raise

if __name__ == "__main__":
    initialize_schema()
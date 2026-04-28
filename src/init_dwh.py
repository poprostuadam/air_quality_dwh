"""
Inicjalizacja schematu Hurtowni Danych (Star Schema: 1 Fakt, 3 Wymiary).
Skrypt automatycznie czyści stare tabele i tworzy nową strukturę.
"""
from loguru import logger
from sqlalchemy import text

from src.db_tools import get_db_engine

# 1. Krok czyszczenia - KOLEJNOŚĆ JEST KRYTYCZNA (najpierw Fakty, potem Wymiary)
DROP_TABLES_SQL = """
IF EXISTS (SELECT * FROM sysobjects WHERE name='Fact_AirQuality' AND xtype='U')
    DROP TABLE Fact_AirQuality;

IF EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Station' AND xtype='U')
    DROP TABLE Dim_Station;

IF EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Pollutant' AND xtype='U')
    DROP TABLE Dim_Pollutant;

IF EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Date' AND xtype='U')
    DROP TABLE Dim_Date;
"""

# 2. Krok tworzenia nowej struktury (Idealnie pod nasz Pandas ETL)
CREATE_TABLES_SQL = """
-- Wymiar 1: Stacje
CREATE TABLE Dim_Station (
    location_id INT PRIMARY KEY,
    station_name NVARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    country NVARCHAR(100)
);

-- Wymiar 2: Zanieczyszczenia
CREATE TABLE Dim_Pollutant (
    parameter_id INT PRIMARY KEY,
    parameter_name NVARCHAR(50)
);

-- Wymiar 3: Czas (Wymagany przez prowadzącego)
CREATE TABLE Dim_Date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT
);

-- Tabela Faktów: Pomiary Jakości Powietrza
CREATE TABLE Fact_AirQuality (
    location_id INT,
    parameter_id INT,
    value FLOAT,
    measured_at DATETIMEOFFSET,
    date_key INT,
    CONSTRAINT FK_Fact_Station FOREIGN KEY (location_id) REFERENCES Dim_Station(location_id),
    CONSTRAINT FK_Fact_Pollutant FOREIGN KEY (parameter_id) REFERENCES Dim_Pollutant(parameter_id),
    CONSTRAINT FK_Fact_Date FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key)
);
"""

# 3. Krok zasilania wymiarów statycznych
# Zmieniamy zakres generowania dat na ostatnie 400 dni
POPULATE_DIMENSIONS_SQL = """
SET NOCOUNT ON;
-- Zasilenie zanieczyszczeń
INSERT INTO Dim_Pollutant (parameter_id, parameter_name) VALUES (1, 'PM10'), (2, 'PM2.5');

-- Wygenerowanie pancernego kalendarza (od 2024 do 2035 roku)
DECLARE @StartDate DATE = '2020-01-01';
DECLARE @EndDate DATE = '2035-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO Dim_Date (date_key, full_date, year, month, day)
    VALUES (
        CAST(FORMAT(@StartDate, 'yyyyMMdd') AS INT),
        @StartDate,
        YEAR(@StartDate),
        MONTH(@StartDate),
        DAY(@StartDate)
    );
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;
"""

def initialize_dwh():
    logger.info("Rozpoczynam resetowanie i inicjalizację schematu Hurtowni Danych...")
    engine = get_db_engine()
    
    try:
        # with engine.begin() automatycznie robi COMMIT na koniec lub ROLLBACK w razie błędu
        with engine.begin() as conn:
            logger.info("1/3 Usuwanie starych tabel (jeśli istnieją)...")
            conn.execute(text(DROP_TABLES_SQL))
            
            logger.info("2/3 Tworzenie nowych tabel (zgodnych z Pandas ETL)...")
            conn.execute(text(CREATE_TABLES_SQL))

            logger.info("3/3 Zasilanie wymiarów (Dim_Pollutant, Dim_Date)...")
            conn.execute(text(POPULATE_DIMENSIONS_SQL))
            
        logger.success("Sukces! Baza jest w 100% gotowa na przyjęcie danych z API.")
    except Exception as e:
        logger.error(f"Błąd inicjalizacji bazy: {e}")
        raise

if __name__ == "__main__":
    initialize_dwh()
# System Monitorowania Jakości Powietrza w Polsce – Projekt Hurtownia Danych 

## Opis Projektu
Celem projektu było zaprojektowanie, zbudowanie i zautomatyzowanie potoku klasy ETL (Extract, Transform, Load), który pobiera, przetwarza i wizualizuje historyczne i bieżące dane o jakości powietrza w Polsce. 

Projekt realizuje pełen cykl życia danych analitycznych: od ekstrakcji z publicznego interfejsu API, poprzez transformację do relacyjnego modelu wielowymiarowego, aż po udostępnienie ich w postaci interaktywnego dashboardu biznesowego.

### Architektura i Stack Technologiczny 
- **Źródło Danych:** OpenAQ API v3 (darmowy interfejs REST API z danymi o zanieczyszczeniach).
- **Język programowania:** Python (wykorzystanie bibliotek: `pandas`, `openaq`, `sqlalchemy`, `loguru`).
- **Baza Danych:** MS SQL Server 2022 (uruchamiany w kontenerze).
- **Orkiestracja:** Apache Airflow (zarządzanie i harmonogramowanie zadań potoku).
- **Narzędzie BI / Wizualizacja:** Dash & Plotly.
- **Infrastruktura:** Docker & Docker Compose, menedżer pakietów `uv`.

### Model Danych (Star Schema)
Hurtownia danych została zaprojektowana zgodnie z klasycznym modelem gwiazdy. Składa się z jednej Tabeli Faktów oraz trzech Tabel Wymiarów:
- `Fact_AirQuality` - centralna tabela przechowująca zmierzone wartości (stężenia PM10, PM2.5).
- `Dim_Station` - wymiar geograficzny (informacje o lokalizacji i współrzędnych stacji).
- `Dim_Pollutant` - słownik mierzonych parametrów.
- `Dim_Date` - precyzyjny wymiar czasu ze sztucznym kluczem (Date Key) pozwalający na analizę trendów.

## Logika Procesu ETL

1. **Extract:** Bezpieczne pobieranie danych z zachowaniem limitów zapytań API (Rate Limiting). Ograniczenie obszaru pobierania do terytorium Polski za pomocą Bounding Box.
2. **Transform (`etl_measurements.py`):** Wykorzystanie biblioteki `pandas` (m.in. funkcji `json_normalize`) do spłaszczania zagnieżdżonych struktur JSON. Czyszczenie brakujących wartości (NaN), naprawa typów danych oraz generowanie analitycznych kluczy obcych.
3. **Load (`load_data.py`):** Bezpieczne zasilanie przyrostowe (Delta Load - dopisywanie nowych rekordów). Wykorzystanie silnika `SQLAlchemy` z parametrem `fast_executemany` w celu realizacji błyskawicznych, masowych zapisów (Bulk Insert) do bazy MS SQL.

## Instrukcja Uruchomienia

Projekt jest w pełni skonteneryzowany, co gwarantuje jego powtarzalne działanie na dowolnym systemie operacyjnym (Windows/Linux/macOS). Został przygotowany z wykorzystaniem nowoczesnego, ultraszybkiego menedżera pakietów `uv`.

### Wymagania wstępne:
* Zainstalowany **Docker** oraz **Docker Compose**.
* Zainstalowany menedżer pakietów **uv** dla języka Python.

### Kroki uruchomieniowe:

**1. Konfiguracja zmiennych środowiskowych**
Utwórz plik `.env` w głównym katalogu projektu i umieść w nim konfigurację:
```text
OPENAQ_API_KEY=twój_klucz_api_z_openaq
AIRFLOW_UID=50000
```

*(Uwaga dla systemów Linux: `AIRFLOW_UID` powinno odpowiadać Twojemu ID użytkownika, co można sprawdzić komendą `id -u`).*

**2. Uruchomienie infrastruktury (Docker)**
Pobranie obrazów i uruchomienie bazy MS SQL oraz wszystkich usług Apache Airflow:
```bash
docker compose up -d
```

**3. Inicjalizacja środowiska i bazy danych**
Instalacja zależności (na podstawie pliku `pyproject.toml`) oraz utworzenie struktury tabel (modelu gwiazdy) w bazie:
```bash
uv sync
uv run python -m src.init_dwh
```

**4. Wstępne zapełnienie bazy danych**
Pobranie historycznych danych z API (Initial Load) i zasilenie tabeli faktów, aby narzędzie raportowe miało bazę do analizy:
```bash
uv run python -m src.initial_load
```

**5. Uruchomienie interfejsu wizualnego (Dashboard)**
Aby włączyć lokalny serwer aplikacji raportowej Dash:
```bash
uv run python -m dashboard.app
```

**Dostęp do aplikacji:**
* **Apache Airflow** (Zarządzanie potokiem ETL): `http://localhost:8080` (Domyślny login/hasło: *admin* / *admin*)
* **Dashboard Analityczny** (Dash): `http://localhost:8050`

## Wnioski i Najciekawsze Rozwiązania Inżynierskie

1. **Obejście ograniczeń API i praca z zagnieżdżonymi strukturami:** Pobieranie danych z OpenAQ API wymagało dwuetapowego, sprytnego podejścia. Endpoint odpowiedzialny za pobieranie samych pomiarów nie pozwala na bezpośrednie filtrowanie po obszarze geograficznym (Bounding Box) – wymaga podania konkretnych identyfikatorów stacji/sensorów. Wymusiło to stworzenie mechanizmu, który najpierw pobiera stacje z danego obszaru, wyciąga z nich ID, a dopiero potem odpytuje API o pomiary. Dodatkowym wyzwaniem było odpowiednie "spłaszczenie" (flattening) głęboko zagnieżdżonych odpowiedzi JSON. Problem ten został rozwiązany przy pomocy funkcji `pd.json_normalize()` z biblioteki Pandas, co pozwoliło uniknąć nieefektywnego iterowania pętlami.
2. **Optymalizacja procesu Load (Bulk Insert):** Początkowe testy ładowania tysięcy wierszy pojedynczymi zapytaniami `INSERT` generowały duże opóźnienia sieciowe. Rozwiązaniem okazało się włączenie parametru `fast_executemany=True` podczas konfiguracji połączenia w bibliotece `SQLAlchemy`. Pozwala to na wysyłanie skompresowanych paczek danych, co drastycznie skróciło czas zasilania hurtowni.
3. **Zalety Konteneryzacji (Docker):** Skonteneryzowanie całego środowiska bazy danych (MS SQL) oraz narzędzia do orkiestracji (Airflow) wyeliminowało klasyczny problem "u mnie działa". Umożliwiło to płynne przeniesienie procesu potoku ETL między systemami Windows oraz Linux (Ubuntu) bez konieczności ponownej instalacji sterowników systemowych dla bazy danych.
4. **Skalowalność Modelu Gwiazdy:** Zaproponowana architektura (Star Schema) sprawia, że system jest niezwykle elastyczny. Dodanie nowych mierzonych zanieczyszczeń (np. O3, NO2) czy stacji z innych krajów wymaga jedynie dopisania nowych rekordów słownikowych do wymiarów, pozostawiając logikę głównej tabeli faktów nietkniętą.
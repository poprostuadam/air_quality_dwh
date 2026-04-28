FROM apache/airflow:2.8.1

# Przełączamy na użytkownika root
USER root

# Instalujemy sterowniki Microsoft SQL Server (ODBC 18)
RUN apt-get update \
    && apt-get install -y curl gnupg2 apt-transport-https \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor --batch --yes -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Wracamy do użytkownika airflow
USER airflow

# Instalujemy biblioteki Pythona
RUN pip install --no-cache-dir pyodbc sqlalchemy loguru pandas requests openaq
"""
Dashboard module for the Air Quality Data Warehouse (DWH).

This module creates an interactive web application using Plotly Dash.
It connects to the MS SQL Server database and features a two-tab interface:
1. Global Analysis: Displays overall pollution trends and a Top 10 ranking.
2. City Analysis: Provides drill-down functionality, allowing users to select 
   a specific station to view its complete historical measurement data with 
   dynamic axis scaling.
"""

import os
import sys

import dash
import pandas as pd
import plotly.express as px
from dash import Input, Output, dcc, html
from loguru import logger

# Add parent directory to path to access 'src' module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.db_tools import get_db_engine


def get_db_connection():
    """
    Retrieves a SQLAlchemy engine instance for database connections.

    Returns:
        Engine: A connected SQLAlchemy engine object.
    """
    return get_db_engine()

def fetch_global_data():
    """
    Fetches aggregated global data for the primary dashboard view.

    Executes two SQL queries against the Data Warehouse:
    1. A time-series trend of average pollutant values grouped by date and parameter.
    2. A Top 10 ranking of stations with the highest average PM10 concentrations.

    Returns:
        tuple: A tuple containing two Pandas DataFrames:
            - df_trend (pd.DataFrame): Time-series data for overall trends.
            - df_top10 (pd.DataFrame): Top 10 most polluted stations.
    """
    engine = get_db_connection()

    df_trend = pd.read_sql("""
        SELECT d.full_date, p.parameter_name, AVG(f.value) as avg_value
        FROM Fact_AirQuality f
        JOIN Dim_Date d ON f.date_key = d.date_key
        JOIN Dim_Pollutant p ON f.parameter_id = p.parameter_id
        GROUP BY d.full_date, p.parameter_name
        ORDER BY d.full_date;
    """, engine)
    
    df_top10 = pd.read_sql("""
        SELECT TOP 10 s.station_name, AVG(f.value) as avg_pm10
        FROM Fact_AirQuality f
        JOIN Dim_Station s ON f.location_id = s.location_id
        JOIN Dim_Pollutant p ON f.parameter_id = p.parameter_id
        WHERE p.parameter_name = 'PM10'
        GROUP BY s.station_name
        ORDER BY avg_pm10 DESC;
    """, engine)
    return df_trend, df_top10

def get_stations_list():
    """
    Retrieves a complete list of available monitoring stations for the UI dropdown.

    Returns:
        pd.DataFrame: A DataFrame containing 'location_id' and 'station_name',
        sorted alphabetically by station name.
    """
    engine = get_db_connection()
    return pd.read_sql("SELECT location_id, station_name FROM Dim_Station ORDER BY station_name", engine)

def fetch_city_details(location_id):
    """
    Fetches the complete historical measurement data for a specific station.
    Utilized for the drill-down view with an adaptive time range.

    Args:
        location_id (int): The unique identifier of the selected station.

    Returns:
        pd.DataFrame: A DataFrame containing dates, pollutant names, and measured values
        for the specified location, ordered chronologically.
    """
    engine = get_db_connection()
    query = f"""
        SELECT d.full_date, p.parameter_name, f.value
        FROM Fact_AirQuality f
        JOIN Dim_Date d ON f.date_key = d.date_key
        JOIN Dim_Pollutant p ON f.parameter_id = p.parameter_id
        WHERE f.location_id = {location_id}
        ORDER BY d.full_date;
    """
    return pd.read_sql(query, engine)


app = dash.Dash(__name__, title="Air Quality DWH", suppress_callback_exceptions=True)

df_trend_glob, df_top10_glob = fetch_global_data()
stations_list = get_stations_list()

app.layout = html.Div(style={'backgroundColor': '#111111', 'color': 'white', 'fontFamily': 'Arial, sans-serif', 'minHeight': '100vh'}, children=[
    html.Header(style={'padding': '20px', 'borderBottom': '1px solid #333'}, children=[
        html.H1("System Monitorowania Jakości Powietrza (DWH)", style={'margin': '0', 'textAlign': 'center'}),
        html.P("Automatyczny ETL -> MS SQL Server -> Dash", style={'textAlign': 'center', 'color': '#888'})
    ]),

    dcc.Tabs(id="main-tabs", value='global-view', children=[
        dcc.Tab(label='ANALIZA OGÓLNA', value='global-view', 
                style={'backgroundColor': '#1a1a1a'}, selected_style={'backgroundColor': '#333', 'fontWeight': 'bold'}),
        dcc.Tab(label='ANALIZA MIASTA', value='city-view', 
                style={'backgroundColor': '#1a1a1a'}, selected_style={'backgroundColor': '#333', 'fontWeight': 'bold'}),
    ]),

    html.Div(id='tabs-content', style={'padding': '30px'})
])


@app.callback(Output('tabs-content', 'children'),
              Input('main-tabs', 'value'))
def render_content(tab):
    """
    Callback function to render the layout dynamically based on the selected tab.

    Args:
        tab (str): The value of the currently selected tab ('global-view' or 'city-view').

    Returns:
        dash.development.base_component.Component: The HTML structure and Graphs 
        corresponding to the active tab.
    """
    if tab == 'global-view':
        return html.Div([
            html.Div([
                dcc.Graph(figure=px.line(df_trend_glob, x='full_date', y='avg_value', color='parameter_name',
                                       title="Średni trend zanieczyszczeń", template="plotly_dark"))
            ], style={'marginBottom': '30px'}),
            
            html.Div([
                dcc.Graph(figure=px.bar(df_top10_glob, x='avg_pm10', y='station_name', orientation='h',
                                      title="TOP 10 najbardziej zanieczyszczonych stacji (PM10)", template="plotly_dark")
                          .update_layout(yaxis={'categoryorder':'total ascending'}))
            ])
        ])

    elif tab == 'city-view':
        return html.Div([
            html.Div(style={'maxWidth': '500px', 'margin': 'auto', 'paddingBottom': '30px'}, children=[
                html.Label("Wybierz miasto / stację pomiarową:"),
                dcc.Dropdown(
                    id='city-dropdown',
                    options=[{'label': row['station_name'], 'value': row['location_id']} for _, row in stations_list.iterrows()],
                    value=stations_list.iloc[0]['location_id'] if not stations_list.empty else None,
                    style={'color': '#000'}
                )
            ]),
            html.Div(id='city-graph-container')
        ])

@app.callback(
    Output('city-graph-container', 'children'),
    Input('city-dropdown', 'value')
)
def update_city_chart(selected_location):
    """
    Callback function to dynamically update the city-specific line chart.

    Fetches new data from the database based on the user's dropdown selection 
    and generates a Plotly figure. Handles empty states gracefully if no data 
    exists for the selected station.

    Args:
        selected_location (int): The location_id selected from the UI dropdown menu.

    Returns:
        dash.development.base_component.Component: A Dash Graph component containing 
        the updated Plotly figure, or an HTML Div warning if data is missing.
    """
    if not selected_location:
        return html.Div("Wybierz stację z listy powyżej.")
    
    df = fetch_city_details(selected_location)
    
    if df.empty:
        return html.Div(style={'textAlign': 'center', 'color': 'orange', 'padding': '50px'}, 
                        children="Brak danych historycznych dla tej stacji w bazie.")

    fig = px.line(df, x='full_date', y='value', color='parameter_name',
                 title=f"Pełna historia pomiarów dla wybranej stacji",
                 template="plotly_dark",
                 labels={'value': 'Stężenie (µg/m³)', 'full_date': 'Data'},
                 markers=True)
    
    fig.update_layout(hovermode="x unified")
    
    return dcc.Graph(figure=fig)

if __name__ == '__main__':
    app.run(debug=True, port=8050)

import os
import argparse
import yfinance as yf
import pandas as pd
from typing import List, Optional
import config

def get_args():
    """
    Obtiene y parsea los argumentos de la línea de comandos.
    """
    parser = argparse.ArgumentParser(description="ETL para datos de yfinance a un archivo CSV.")
    parser.add_argument(
        '--tickers',
        nargs='+',
        default=config.TICKERS,
        help=f'Lista de tickers a procesar. Ejemplo: --tickers AAPL GOOG. Default: {config.TICKERS}'
    )
    parser.add_argument(
        '--start_date',
        default=config.START_DATE,
        help=f'Fecha de inicio para la descarga de datos (YYYY-MM-DD). Default: {config.START_DATE}'
    )
    parser.add_argument(
        '--end_date',
        default=config.END_DATE,
        help=f'Fecha de fin para la descarga de datos (YYYY-MM-DD). Default: {config.END_DATE}'
    )
    return parser.parse_args()

def extract_data(tickers: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    Descarga datos de yfinance para los tickers y rango de fechas especificados.
    """
    print(f"Paso 1: Descargando datos para {tickers} desde yfinance...")
    try:
        data = yf.download(tickers, start=start_date, end=end_date)
        if data.empty:
            print("No se descargaron datos. Verifique los tickers y el rango de fechas.")
            return None
        print("Datos descargados exitosamente.")
        return data
    except Exception as e:
        print(f"Error al descargar datos de yfinance: {e}")
        return None

def transform_data(data: pd.DataFrame, tickers: List[str]) -> Optional[pd.DataFrame]:
    """
    Transforma los datos descargados para calcular los rendimientos diarios.
    """
    print("Paso 2: Transformando los datos...")
    try:
        if len(tickers) == 1:
            adj_close = data['Close'].to_frame(name=tickers[0])
        else:
            adj_close = data['Close']

        daily_returns = adj_close.pct_change().dropna()

        value_vars = tickers
        if isinstance(daily_returns, pd.DataFrame) and len(daily_returns.columns) == 1:
            value_vars = daily_returns.columns.tolist()

        df_final = daily_returns.reset_index().melt(
            id_vars=['Date'],
            value_vars=value_vars,
            var_name='ticker',
            value_name='daily_return'
        )
        df_final.rename(columns={'Date': 'date'}, inplace=True)
        
        print("Datos transformados exitosamente.")
        return df_final
    except Exception as e:
        print(f"Error durante la transformación de datos: {e}")
        return None

def save_to_csv(df: pd.DataFrame, filename: str = 'rendimientos_diarios.csv'):
    """
    Guarda el DataFrame en un archivo CSV.
    """
    print(f"Paso 3: Guardando datos en {filename}...")
    try:
        df.to_csv(filename, index=False)
        print(f"Datos guardados exitosamente en {filename}.")
    except Exception as e:
        print(f"Ocurrió un error al guardar el archivo CSV: {e}")

def main():
    """
    Proceso ETL principal.
    """
    print("Iniciando el proceso ETL para CSV...")
    args = get_args()
    
    data = extract_data(args.tickers, args.start_date, args.end_date)
    if data is not None:
        transformed_data = transform_data(data, args.tickers)
        if transformed_data is not None:
            save_to_csv(transformed_data)

if __name__ == "__main__":
    main()

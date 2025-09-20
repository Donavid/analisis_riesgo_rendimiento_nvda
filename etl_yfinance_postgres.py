import os
import argparse
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import List, Optional
import config

def get_args():
    """
    Obtiene y parsea los argumentos de la línea de comandos.
    """
    parser = argparse.ArgumentParser(description="ETL para datos de yfinance a PostgreSQL.")
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
    parser.add_argument(
        '--db_host',
        default=os.getenv("POSTGRES_HOST", config.DB_HOST),
        help=f"Host de la base de datos. Default: {config.DB_HOST}"
    )
    parser.add_argument(
        '--db_name',
        default=os.getenv("POSTGRES_DB", config.DB_NAME),
        help=f"Nombre de la base de datos. Default: {config.DB_NAME}"
    )
    parser.add_argument(
        '--db_user',
        default=os.getenv("POSTGRES_USER", config.DB_USER),
        help=f"Usuario de la base de datos. Default: {config.DB_USER}"
    )
    parser.add_argument(
        '--db_password',
        default=os.getenv("POSTGRES_PASSWORD", config.DB_PASSWORD),
        help="Contraseña de la base de datos. Default: password en config.py"
    )
    parser.add_argument(
        '--table_name',
        default=config.TABLE_NAME,
        help=f"Nombre de la tabla donde se cargarán los datos. Default: {config.TABLE_NAME}"
    )
    parser.add_argument(
        '--sslmode',
        default=config.SSL_MODE,
        help=f"Modo SSL de la conexión a la base de datos (e.g., 'require', 'disable'). Default: {config.SSL_MODE}"
    )
    return parser.parse_args()

def extract_data(tickers: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    Descarga datos de yfinance para los tickers y rango de fechas especificados.

    Args:
        tickers: Lista de tickers a descargar.
        start_date: Fecha de inicio (YYYY-MM-DD).
        end_date: Fecha de fin (YYYY-MM-DD).

    Returns:
        Un DataFrame de pandas con los datos descargados, o None si ocurre un error.
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

    Args:
        data: DataFrame con los datos de yfinance.
        tickers: Lista de tickers.

    Returns:
        Un DataFrame de pandas con los datos transformados, o None si ocurre un error.
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
        print("Vista previa de los datos a cargar:")
        print(df_final.head())
        return df_final
    except Exception as e:
        print(f"Error durante la transformación de datos: {e}")
        return None

def create_table_if_not_exists(engine, table_name):
    """
    Crea la tabla en la base de datos si no existe.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP,
        ticker VARCHAR(10),
        daily_return FLOAT
    );
    """
    try:
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(create_table_query))
        print(f"Tabla '{table_name}' verificada/creada exitosamente.")
    except Exception as e:
        print(f"Error al crear la tabla '{table_name}': {e}")
        raise

def load_data(df: pd.DataFrame, db_host: str, db_name: str, db_user: str, db_password: str, table_name: str, sslmode: str):
    """
    Carga los datos transformados en la base de datos PostgreSQL.

    Args:
        df: DataFrame con los datos a cargar.
        db_host: Host de la base de datos.
        db_name: Nombre de la base de datos.
        db_user: Usuario de la base de datos.
        db_password: Contraseña de la base de datos.
        table_name: Nombre de la tabla.
        sslmode: Modo SSL de la conexión.
    """
    print(f"Paso 3: Cargando datos en PostgreSQL (Host: {db_host})...")
    
    if not db_password:
        print("Error: La contraseña de la base de datos no está configurada.")
        return

    db_uri = f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}?sslmode={sslmode}"

    try:
        engine = create_engine(db_uri)
        
        # Crear tabla si no existe
        create_table_if_not_exists(engine, table_name)

        with engine.connect() as connection:
            with connection.begin():
                print(f"Vaciando la tabla '{table_name}'...")
                connection.execute(text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY;'))
                print("Tabla vaciada exitosamente.")

                df.to_sql(
                    table_name,
                    connection,
                    if_exists='append',
                    index=False
                )
        print(f"¡Proceso ETL completado! Los datos han sido cargados en la tabla '{table_name}'.")
    except OperationalError as e:
        print(f"Error de conexión a PostgreSQL: {e}")
        print("Por favor, verifique las credenciales y la conexión a la base de datos.")
    except Exception as e:
        print(f"Ocurrió un error al cargar datos en PostgreSQL: {e}")

def main():
    """
    Proceso ETL principal.
    """
    print("Iniciando el proceso ETL...")
    args = get_args()
    
    data = extract_data(args.tickers, args.start_date, args.end_date)
    if data is not None:
        transformed_data = transform_data(data, args.tickers)
        if transformed_data is not None:
            load_data(
                transformed_data,
                args.db_host,
                args.db_name,
                args.db_user,
                args.db_password,
                args.table_name,
                args.sslmode
            )

if __name__ == "__main__":
    main()
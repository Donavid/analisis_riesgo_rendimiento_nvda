
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# --- Funciones de Análisis ---

def calculate_statistics(df):
    """Calcula estadísticas descriptivas para cada ticker."""
    stats = df.groupby('ticker')['daily_return'].describe()
    return stats

def calculate_cumulative_returns(df):
    """Calcula los rendimientos acumulados."""
    df['cumulative_return'] = (1 + df['daily_return']).cumprod() - 1
    return df

def calculate_annualized_volatility(df):
    """Calcula la volatilidad anualizada."""
    volatility = df.groupby('ticker')['daily_return'].std() * np.sqrt(252)
    return volatility.reset_index(name='annualized_volatility')

def calculate_sharpe_ratio(df, risk_free_rate=0.0):
    """Calcula el Sharpe Ratio."""
    mean_return = df.groupby('ticker')['daily_return'].mean() * 252
    std_dev = df.groupby('ticker')['daily_return'].std() * np.sqrt(252)
    sharpe_ratio = (mean_return - risk_free_rate) / std_dev
    return sharpe_ratio.reset_index(name='sharpe_ratio')

def calculate_correlation(df_pivot):
    """Calcula la correlación entre los tickers."""
    return df_pivot.corr()

# --- Funciones para Generar HTML ---

def get_header_style():
    return """<style>
        body { font-family: Arial, sans-serif; margin: 40px; background-color: #f4f4f9; color: #333; }
        h1, h2 { color: #1a1a2e; }
        h1 { text-align: center; }
        .container { background-color: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 0 15px rgba(0,0,0,0.1); }
        .table-container { overflow-x: auto; margin-bottom: 30px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px 15px; border: 1px solid #ddd; text-align: left; }
        th { background-color: #1a1a2e; color: #fff; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .conclusion { background-color: #e6f7ff; border-left: 5px solid #1a1a2e; padding: 20px; margin-top: 30px; }
    </style>"""

def generate_html_report(stats, cumulative_returns_fig, volatility, sharpe_ratio, correlation_matrix, rolling_vol_fig, output_path):
    """Genera un informe HTML con los resultados del análisis."""
    html_content = f"""<html>
<head>
    <title>Análisis Comparativo de Rendimiento: NVDA vs QQQ</title>
    {get_header_style()}
</head>
<body>
    <div class="container">
        <h1>Análisis Comparativo de Rendimiento: NVDA vs QQQ (2019-2024)</h1>
        
        <h2>1. Estadísticas Descriptivas de Rendimientos Diarios</h2>
        <div class="table-container">{stats.to_html()}</div>
        
        <h2>2. Rendimiento Acumulado</h2>
        <div>{cumulative_returns_fig.to_html(full_html=False, include_plotlyjs='cdn')}</div>
        
        <h2>3. Métricas de Riesgo y Rendimiento Anualizado</h2>
        <div class="table-container">
            <h3>Volatilidad Anualizada</h3>
            {volatility.to_html(index=False)}
            <h3>Sharpe Ratio (Rendimiento ajustado al Riesgo)</h3>
            {sharpe_ratio.to_html(index=False)}
        </div>
        
        <h2>4. Correlación de Rendimientos Diarios</h2>
        <div class="table-container">{correlation_matrix.to_html()}</div>

        <h2>5. Volatilidad Móvil (Rolling Volatility) - 30 Días</h2>
        <div>{rolling_vol_fig.to_html(full_html=False, include_plotlyjs='cdn')}</div>

        <div class="conclusion">
            <h2>Conclusiones Clave</h2>
            <p><strong>Rendimiento:</strong> NVDA ha mostrado un rendimiento acumulado significativamente superior al del índice QQQ en el período analizado. Esto se refleja en una media de rendimiento diario más alta.</p>
            <p><strong>Volatilidad:</strong> La mayor rentabilidad de NVDA viene acompañada de una volatilidad considerablemente más alta. Esto indica que, si bien el potencial de ganancia ha sido mayor, el riesgo también lo ha sido.</p>
            <p><strong>Rendimiento ajustado al Riesgo:</strong> A pesar de su alta volatilidad, el Sharpe Ratio de NVDA es superior al de QQQ. Esto sugiere que NVDA ha compensado a los inversores de manera más efectiva por el riesgo asumido.</p>
            <p><strong>Correlación:</strong> Existe una correlación positiva y relativamente fuerte entre NVDA y QQQ. Esto es esperado, ya que NVDA es un componente importante del NASDAQ-100. Sin embargo, la correlación no es perfecta, lo que indica que factores específicos de la empresa también influyen fuertemente en el rendimiento de NVDA.</p>
        </div>
    </div>
</body>
</html>"""
    
    with open(output_path, 'w') as f:
        f.write(html_content)
    print(f"Informe HTML generado exitosamente en: {output_path}")

def main():
    # --- Carga y Preparación de Datos ---
    try:
        df = pd.read_csv('rendimientos_diarios.csv', parse_dates=['date'])
    except FileNotFoundError:
        print("Error: El archivo 'rendimientos_diarios.csv' no fue encontrado.")
        return

    df_pivot = df.pivot(index='date', columns='ticker', values='daily_return')

    # --- Análisis ---
    stats = calculate_statistics(df)
    volatility = calculate_annualized_volatility(df)
    sharpe_ratio = calculate_sharpe_ratio(df)
    correlation_matrix = calculate_correlation(df_pivot)

    # --- Gráficos ---
    # Rendimiento Acumulado
    df_cumulative = df.groupby(['date', 'ticker'])['daily_return'].sum().unstack().cumsum()
    df_cumulative = (1 + df_pivot).cumprod() - 1

    cumulative_returns_fig = go.Figure()
    for ticker in df_cumulative.columns:
        cumulative_returns_fig.add_trace(go.Scatter(x=df_cumulative.index, y=df_cumulative[ticker], mode='lines', name=ticker))
    cumulative_returns_fig.update_layout(title='Rendimiento Acumulado (2019-2024)', xaxis_title='Fecha', yaxis_title='Rendimiento Acumulado')

    # Volatilidad Móvil
    rolling_vol = df_pivot.rolling(window=30).std() * np.sqrt(252)
    rolling_vol_fig = go.Figure()
    for ticker in rolling_vol.columns:
        rolling_vol_fig.add_trace(go.Scatter(x=rolling_vol.index, y=rolling_vol[ticker], mode='lines', name=ticker))
    rolling_vol_fig.update_layout(title='Volatilidad Móvil de 30 días', xaxis_title='Fecha', yaxis_title='Volatilidad Anualizada')

    # --- Generación de Informe ---
    output_path = '/home/fo5/Escritorio/analisis_nvda_qqq.html'
    generate_html_report(stats, cumulative_returns_fig, volatility, sharpe_ratio, correlation_matrix, rolling_vol_fig, output_path)

if __name__ == "__main__":
    main()

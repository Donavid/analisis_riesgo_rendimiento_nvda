# analisis_riesgo_rendimiento_nvda
Análisis comparativo del rendimiento y la volatilidad de NVIDIA (NVDA) frente al índice NASDAQ-100 (QQQ) usando Python, yfinance y PostgreSQL.
# Análisis de Riesgo y Rendimiento: NVIDIA (NVDA) vs. NASDAQ-100 (QQQ)

Este proyecto realiza un análisis financiero comparativo completo entre las acciones de NVIDIA y el índice NASDAQ-100, evaluando tanto el rendimiento histórico como la volatilidad. El objetivo es determinar si el mayor riesgo de NVIDIA ha sido compensado por un mayor rendimiento.

## 📊 Dashboard Interactivo

**Puedes explorar el dashboard final e interactivo en el siguiente enlace:**

[**Ver el Dashboard en Looker Studio**](https://lookerstudio.google.com/reporting/f95ad12f-a580-4e5f-9147-3964ec43fba7)

---

## 🛠️ Arquitectura y Flujo de Trabajo

El proyecto sigue un flujo de trabajo de datos moderno, desde la ingesta hasta la visualización:

1.  **Extracción (ETL):** Un script de **Python** (`etl_yfinance_postgres.py`) utiliza la librería `yfinance` para descargar 5 años de datos históricos de precios.
2.  **Almacenamiento:** Los datos son cargados en una base de datos **PostgreSQL** alojada en **AWS RDS** para un almacenamiento robusto y escalable.
3.  **Transformación (SQL):** Se utilizan **Vistas (Views)** de SQL (`sql_scripts/create_views.sql`) para calcular métricas financieras complejas directamente en la base de datos, incluyendo:
    *   Rendimiento Acumulado
    *   Volatilidad Móvil Anualizada (30 días)
    *   Sharpe Ratio
4.  **Visualización:** Un dashboard interactivo creado en **Looker Studio** se conecta directamente a la base de datos en AWS, permitiendo una visualización de datos siempre actualizada.

---

## 🚀 Stack Tecnológico

*   **Lenguajes:** Python, SQL
*   **Base de Datos:** PostgreSQL en AWS RDS
*   **ETL:** yfinance, Pandas, SQLAlchemy
*   **BI / Visualización:** Looker Studio
*   **Control de Versiones:** Git & GitHub

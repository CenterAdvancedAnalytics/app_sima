import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

#%%% Funcion 2
def usarios_acontecimientos_dashboard():
    st.title("Usuarios y Acontecimientos - Análisis Diario")

    # Cargar las tablas desde los archivos
    usuarios_por_dia = pd.read_parquet('app/tables/temp_usuarios_por_dia.parquet')
    acontecimientos_por_dia = pd.read_parquet('app/tables/temp_acontecimientos_por_dia.parquet')
    usuarios_ultimo_dia = pd.read_parquet('app/tables/temp_usuarios_ultimo_dia.parquet')
    usuarios_semana = pd.read_parquet('app/tables/temp_usuarios_7_dias.parquet')

    # Formatear las columnas de fecha
    # usuarios_por_dia['fecha'] = pd.to_datetime(usuarios_por_dia['fecha'])
    # acontecimientos_por_dia['fecha'] = pd.to_datetime(acontecimientos_por_dia['fecha'])
    # usuarios_ultimo_dia['ultima_actualizacion'] = pd.to_datetime(usuarios_ultimo_dia['ultima_actualizacion'])

    usuarios_por_dia['fecha'] = usuarios_por_dia['fecha'].map(str)
    hoy = datetime.now()
    hace_30_dias = hoy - timedelta(days=90)
    temp = pd.DataFrame()
    temp['fecha'] = pd.date_range(hace_30_dias,hoy,freq='D')
    temp['fecha'] = temp['fecha'].map(str).str[:10]
    temp = temp.merge(usuarios_por_dia,how='left',on='fecha')
    temp['fecha'] = pd.to_datetime(temp['fecha'])
    temp['usuarios_distintos'] = temp['usuarios_distintos'].fillna(0)
    usuarios_por_dia = temp

    acontecimientos_por_dia['fecha'] = acontecimientos_por_dia['fecha'].map(str)
    hoy = datetime.now()
    hace_30_dias = hoy - timedelta(days=90)
    temp = pd.DataFrame()
    temp['fecha'] = pd.date_range(hace_30_dias,hoy,freq='D')
    temp['fecha'] = temp['fecha'].map(str).str[:10]
    temp = temp.merge(acontecimientos_por_dia,how='left',on='fecha')
    temp['fecha'] = pd.to_datetime(temp['fecha'])
    temp['total_acontecimientos'] = temp['total_acontecimientos'].fillna(0)
    acontecimientos_por_dia = temp

    # Renombrar las columnas en usuarios_semana
    hoy = datetime.now().date()
    fechas = [(hoy - timedelta(days=6 - i)).strftime("%d-%m-%Y") for i in range(7)]
    mapeo_columnas = {f"dia_{i+1}": fechas[i] for i in range(7)}
    usuarios_semana = usuarios_semana.rename(columns=mapeo_columnas)

    # Visualización de los datos
    st.subheader("Usuarios que registran acontecimientos por día")
    st.line_chart(usuarios_por_dia.set_index('fecha')['usuarios_distintos'])

    st.subheader("Acontecimientos registrados por día")
    st.line_chart(acontecimientos_por_dia.set_index('fecha')['total_acontecimientos'])

    st.subheader("Usuarios que registraron acontecimientos el ultimo dia de la base de datos")
    st.dataframe(usuarios_ultimo_dia)

    st.subheader("Numero de acontecimientos registrados por usuario y por dia en los últimos 7 días")
    st.dataframe(usuarios_semana)


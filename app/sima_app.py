#%% Libraries
import streamlit as st
import os
from datetime import datetime, timedelta
import pytz
from function_cocteles import coctel_dashboard
from function_users import usarios_acontecimientos_dashboard

#%%% Formateo del nombre del sitio y site config
st.set_page_config(page_title="Dashboard SIMA", layout="centered")
st.title("Dashboard SIMA")
st.divider()

#%%%% Agregar opción al menú lateral
peru_tz = pytz.timezone("America/Lima")
file_path = "app/tables/temp_base_coctel.parquet"
if os.path.exists(file_path):
    last_updated = os.path.getmtime(file_path)
    # Convertir a datetime con hora UTC y luego ajustar a la zona horaria de Perú
    last_updated_date = datetime.fromtimestamp(last_updated, pytz.utc).astimezone(peru_tz).strftime('%d/%m/%Y %H:%M:%S')
else:
    last_updated_date = "Archivo no encontrado"

st.sidebar.title("Opciones")
st.sidebar.write(f"Fecha de última actualización de datos: {last_updated_date}")
menu = st.sidebar.radio(
    "Selecciona una sección",
    ["Análisis de Cocteles", "Usuarios y Acontecimientos"]
)

if menu == "Análisis de Cocteles":
    coctel_dashboard()

elif menu == "Usuarios y Acontecimientos":
    usarios_acontecimientos_dashboard()


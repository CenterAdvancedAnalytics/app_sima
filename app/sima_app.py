#%%
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
# from update_tables import update_data
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta
import pytz

# Botón para actualizar los datos
st.set_page_config(page_title="Dashboard SIMA", layout="centered")

#%%% Formateo del nombre del sitio y site config
st.title("Dashboard SIMA")
st.divider()
    
#%%%% Definir funciones
#%%% def funcion usuarios
def usarios_acontecimientos_dashboard():
    st.title("Usuarios y Acontecimientos - Análisis Diario")

    # Cargar las tablas desde los archivos
    usuarios_por_dia = pd.read_parquet('app/tables/temp_usuarios_por_dia.parquet')
    acontecimientos_por_dia = pd.read_parquet('app/tables/temp_acontecimientos_por_dia.parquet')
    usuarios_ultimo_dia = pd.read_parquet('app/tables/temp_usuarios_ultimo_dia.parquet')
    usuarios_semana = pd.read_parquet('app/tables/temp_usuarios_7_dias.parquet')

    # Formatear las columnas de fecha
    usuarios_por_dia['fecha'] = pd.to_datetime(usuarios_por_dia['fecha'])
    acontecimientos_por_dia['fecha'] = pd.to_datetime(acontecimientos_por_dia['fecha'])
    
    # Seleccionar solo las últimas 30 observaciones
    usuarios_por_dia = usuarios_por_dia.tail(30)
    acontecimientos_por_dia = acontecimientos_por_dia.tail(30)

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

    st.subheader("Usuarios que registraron acontecimientos en los últimos 7 días")
    st.dataframe(usuarios_semana)

#%%% def funcion cocteles
def coctel_dashboard():
    st.title("Análisis de Cocteles")
    temp_coctel_fuente = pd.read_parquet('app/tables/temp_base_coctel.parquet')
    temp_coctel_fuente_programas = temp_coctel_fuente
    temp_coctel_fuente_fb = pd.read_parquet('app/tables/temp_coctel_fuente_fb.parquet')
    temp_coctel_fuente_actores = pd.read_parquet('app/tables/temp_coctel_fuente_actores.parquet')
    temp_coctel_temas = pd.read_parquet('app/tables/temp_coctel_temas.parquet')

    lugares_uniques = temp_coctel_fuente['lugar'].unique().tolist()

    temp_coctel_fuente['fecha_registro'] = pd.to_datetime(temp_coctel_fuente['fecha_registro'])
    temp_coctel_fuente_programas['fecha_registro'] = pd.to_datetime(temp_coctel_fuente_programas['fecha_registro'])
    temp_coctel_fuente_fb['fecha_registro'] = pd.to_datetime(temp_coctel_fuente_fb['fecha_registro'])
    temp_coctel_fuente_actores['fecha_registro'] = pd.to_datetime(temp_coctel_fuente_actores['fecha_registro'])
    temp_coctel_temas['fecha_registro'] = pd.to_datetime(temp_coctel_temas['fecha_registro'])

    #%% diccionarios

    id_posicion_dict = {1: 'a favor',
                        2: 'potencialmente a favor',
                        3: 'neutral',
                        4: 'potencialmente en contra',
                        5: 'en contra'}
    coctel_dict = {0: 'Sin coctel',
                1: 'Con coctel'
                }
    id_fuente_dict = {1: 'Radio',
                    2: 'TV',
                    3: 'Redes'
                    }
    color_posicion_dict = {"a favor": "blue",
                        "potencialmente a favor": "lightblue",
                        "neutral": "gray",
                        "potencialmente en contra": "#FFA500",
                        "en contra": "red"
                        }
    color_discrete_map = {'Celeste': 'lightblue',
                            'Rojo': 'Red',     
                            'Naranja': '#FFA500',
                            'Gris': 'Gray',
                            'Azul': 'Blue',
                        }

    #% 1.- PROPORCION COCTELES solo fechas y lugar
    st.subheader("sn.- Proporción de cocteles en lugar y fecha específica")
    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_c1 = st.date_input(
        "Fecha Inicio",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_c1 = st.date_input(
        "Fecha Fin",
        format="DD.MM.YYYY")
    with col3:
        option_lugar_c1 = st.selectbox(
        "Lugar",
        lugares_uniques)

    fecha_inicio_c1 = pd.to_datetime(fecha_inicio_c1,format='%Y-%m-%d')
    fecha_fin_c1 = pd.to_datetime(fecha_fin_c1,format='%Y-%m-%d')

    temp_c1 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_c1)&
                                (temp_coctel_fuente['fecha_registro']<=fecha_fin_c1)&
                                (temp_coctel_fuente['lugar']==option_lugar_c1)]

    temp_c1["Fuente"] = temp_c1["id_fuente"].map(id_fuente_dict)

    def proporcion_cocteles(df):
        df = df.rename(columns={'coctel':'Fuente','id':'Cantidad'})
        df['Proporción'] = df['Cantidad'] / df['Cantidad'].sum()
        df['Proporción'] = df['Proporción'].map('{:.0%}'.format)
        df['Fuente'] = df['Fuente'].replace({0:'Otras Fuentes',1:'Coctel Noticias'})
        return df

    if not temp_c1.empty:
        temp_c1_radio = temp_c1[temp_c1['id_fuente']==1].groupby('coctel').agg({'id':'count'}).reset_index()
        temp_c1_radio = proporcion_cocteles(temp_c1_radio)

        temp_c1_tv = temp_c1[temp_c1['id_fuente']==2].groupby('coctel').agg({'id':'count'}).reset_index()
        temp_c1_tv = proporcion_cocteles(temp_c1_tv)

        temp_c1_redes = temp_c1[temp_c1['id_fuente']==3].groupby('coctel').agg({'id':'count'}).reset_index()
        temp_c1_redes = proporcion_cocteles(temp_c1_redes)
        st.write(f"Proporción de cocteles en {option_lugar_c1} entre {fecha_inicio_c1.strftime('%d.%m.%Y')} y {fecha_fin_c1.strftime('%d.%m.%Y')}")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.write("Radio")
            st.dataframe(temp_c1_radio, hide_index=True)

        with col2:
            st.write("TV")
            st.dataframe(temp_c1_tv, hide_index=True)

        with col3:
            st.write("Redes")
            st.dataframe(temp_c1_redes, hide_index=True)

    else:
        st.warning("No hay datos para mostrar")
    #%% 1.- PROPORCION COCTELES

    st.subheader("1.- Proporción de cocteles en lugar, fuentes y fechas específicas")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_t1 = st.date_input(
        "Fecha Inicio g1",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_t1 = st.date_input(
        "Fecha Fin g1",
        format="DD.MM.YYYY")
    with col3:
        option_fuente_t1 = st.multiselect(
        "Fuente g1",
        ["Radio", "TV", "Redes"], ["Radio", "TV", "Redes"])

    option_lugar_t1 = st.multiselect("Lugar g1",
                                    lugares_uniques,
                                    lugares_uniques
                                    )

    fecha_inicio_t1 = pd.to_datetime(fecha_inicio_t1,format='%Y-%m-%d')
    fecha_fin_t1 = pd.to_datetime(fecha_fin_t1,format='%Y-%m-%d')

    temp_t1 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_t1)&
                                (temp_coctel_fuente['fecha_registro']<=fecha_fin_t1)&
                            (temp_coctel_fuente['lugar'].isin(option_lugar_t1))]

    temp_t1["Fuente"] = temp_t1["id_fuente"].map(id_fuente_dict)

    if option_fuente_t1:
        temp_t1 = temp_t1[temp_t1['Fuente'].isin(option_fuente_t1)]

    temp_t1 = temp_t1.groupby('coctel').agg({'id':'count'}).reset_index()

    st.write(f"Proporción de cocteles en {', '.join(option_lugar_t1)} entre {fecha_inicio_t1.strftime('%d.%m.%Y')} y {fecha_fin_t1.strftime('%d.%m.%Y')}")
    if not temp_t1.empty:
        temp_t1 = temp_t1.rename(columns={'coctel':'Fuente','id':'Cantidad'})
        temp_t1['Proporción'] = temp_t1['Cantidad'] / temp_t1['Cantidad'].sum()
        temp_t1['Proporción'] = temp_t1['Proporción'].map('{:.0%}'.format)
        temp_t1['Fuente'] = temp_t1['Fuente'].replace({0:'Otras Fuentes',1:'Coctel Noticias'})
        st.dataframe(temp_t1, hide_index=True)
    else:
        st.warning("No hay datos para mostrar")

    #%% 2.- Posición por fuente

    st.subheader("2.- Posición por fuente en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_t2 = st.date_input(
        "Fecha Inicio g2",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_t2 = st.date_input(
        "Fecha Fin g2",
        format="DD.MM.YYYY")
    with col3:
        option_coctel_t2 = st.selectbox(
        "Notas g2",
        ("Coctel noticias", "Otras fuentes", "Todas"),)
    with col4:
        option_lugar_t2 = st.selectbox(
        "Lugar g2",
        lugares_uniques)

    fecha_inicio_t2 = pd.to_datetime(fecha_inicio_t2,format='%Y-%m-%d')
    fecha_fin_t2 = pd.to_datetime(fecha_fin_t2,format='%Y-%m-%d')

    temp_t2 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_t2)&(temp_coctel_fuente['fecha_registro']<=fecha_fin_t2)&
                            (temp_coctel_fuente['lugar']==option_lugar_t2)]
    temp_colores = pd.DataFrame({'id_fuente': [1,1,1,1,1,2,2,2,2,2,3,3,3,3,3], 
                                'Posicion': ['(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente','(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente','(A) A favor','(E) En contra','(C) Neutral','(B) Potencialmente','(D) Potencialmente'],
                                'color': ['Azul','Rojo','Gris','Celeste','Naranja','Azul','Rojo','Gris','Celeste','Naranja','Azul','Rojo','Gris','Celeste','Naranja']})
    if option_coctel_t2 == 'Coctel noticias':
        temp_t2 = temp_t2[temp_t2['coctel']==1].groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()
    elif option_coctel_t2 == 'Otras fuentes':
        temp_t2 = temp_t2[temp_t2['coctel']==0].groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()
    else:
        temp_t2 = temp_t2.groupby(['id_fuente','color']).agg({'id':'count'}).reset_index()

    st.write(f"Posición por {option_coctel_t2} en {option_lugar_t2} entre {fecha_inicio_t2} y {fecha_fin_t2}")

    if not temp_t2.empty:
        temp_t2 = pd.merge(temp_colores,temp_t2,how='left',on=['id_fuente','color'])
        temp_t2['id'] = temp_t2['id'].fillna(0)
        temp_t2 = temp_t2.rename(columns={'id_fuente':'Medio','color':'Color','id':'Cantidad'})
        temp_t2['Medio'] = temp_t2['Medio'].replace({1:'RADIO',2:'TV',3:'REDES'})
        temp_t2['Porcentaje'] = temp_t2['Cantidad'] / temp_t2.groupby('Medio')['Cantidad'].transform('sum')
        temp_t2['Porcentaje'] = temp_t2['Porcentaje'].map('{:.0%}'.format)
        st.dataframe(temp_t2, hide_index=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% 3.1.- Grafico semanal por porcentaje cocteles

    st.subheader("3.- Gráfico semanal por porcentaje de cocteles en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g1 = st.date_input(
        "Fecha Inicio g3",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g1 = st.date_input(
        "Fecha Fin g3",
        format="DD.MM.YYYY")

    with col3:
        option_fuente_g1 = st.selectbox(
        "Fuente g3",
        ("Radio", "TV", "Redes","Todos"))

    with col4:
        option_lugar_g1 = st.selectbox(
        "Lugar g3",
        lugares_uniques
        )

    fecha_inicio_g1 = pd.to_datetime(fecha_inicio_g1,format='%Y-%m-%d')
    fecha_fin_g1 = pd.to_datetime(fecha_fin_g1,format='%Y-%m-%d')

    temp_g1 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_g1)&(temp_coctel_fuente['fecha_registro']<=fecha_fin_g1)&
                            (temp_coctel_fuente['lugar']==option_lugar_g1)]

    if option_fuente_g1 == 'Radio':
        temp_g1 = temp_g1[temp_g1['id_fuente']==1]
    elif option_fuente_g1 == 'TV':
        temp_g1 = temp_g1[temp_g1['id_fuente']==2]
    elif option_fuente_g1 == 'Redes':
        temp_g1 = temp_g1[temp_g1['id_fuente']==3]


    if not temp_g1.empty:
        temp_g1['semana'] = temp_g1['fecha_registro'].dt.isocalendar().year.map(str) +'-'+temp_g1['fecha_registro'].dt.isocalendar().week.map(str)
        temp_g1 = temp_g1.groupby('semana').agg({'id':'count','coctel':'sum','fecha_registro':'first'}).reset_index()
        temp_g1 = temp_g1.rename(columns={'id':'Cantidad'})
        temp_g1['porcentaje'] = temp_g1['coctel'] / temp_g1['Cantidad']
        temp_g1 = temp_g1.sort_values('fecha_registro')
        temp_fecha = pd.DataFrame()
        temp_fecha['fecha'] = pd.date_range(start=fecha_inicio_g1, end=fecha_fin_g1)
        temp_fecha['semana'] = temp_fecha['fecha'].dt.isocalendar().year.map(str) +'-'+temp_fecha['fecha'].dt.isocalendar().week.map(str)
        temp_fecha = temp_fecha.groupby('semana').agg({'fecha':'first'}).reset_index()
        temp_fecha = temp_fecha.sort_values('fecha')
        del temp_fecha['fecha']
        temp_g1 = pd.merge(temp_fecha, temp_g1, how='left', on='semana')
        temp_g1['Cantidad'] = temp_g1['Cantidad'].fillna(0)
        temp_g1['porcentaje'] = temp_g1['porcentaje'].fillna(0)
        temp_g1 = temp_g1.sort_values('fecha_registro')
        temp_g1['semana'] = temp_g1['semana'].map(str)
        temp_g1["porcentaje"] = temp_g1["porcentaje"] * 100 # a dos decimales
        temp_g1["porcentaje"] = temp_g1["porcentaje"].map('{:.2f}'.format)

        st.write(f"Gráfico semanal por porcentaje de cocteles en {option_lugar_g1} entre {fecha_inicio_g1} y {fecha_fin_g1}")

        fig1 = go.Figure()
        fig1.add_trace(
                go.Scatter(x=temp_g1['semana'], y=temp_g1['porcentaje'], mode='lines+markers'))
        fig1.update_xaxes(type = "category",
                        title_text='Semana'
                        )
        fig1.update_yaxes(title_text='Porcentaje de cocteles %')
        st.plotly_chart(fig1)

    else:
        st.warning("No hay datos para mostrar")

    #%% 3.2.- Grafico semanal noticias a favor y en contra

    st.subheader("4.- Gráfico semanal de noticias a favor y en contra en lugar y fecha específica")

    col1, col2, col3, col4= st.columns(4)
    with col1:
        fecha_inicio_g2 = st.date_input(
        "Fecha Inicio g4",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g2 = st.date_input(
        "Fecha Fin g4",
        format="DD.MM.YYYY")
    with col3:
        option_lugar_g2 = st.selectbox(
        "Lugar g4",
        lugares_uniques)
    with col4:
        option_fuente_g2 = st.selectbox(
        "Fuente g4",
        ("Radio", "TV", "Redes","Todos"))

    fecha_inicio_g2 = pd.to_datetime(fecha_inicio_g2,format='%Y-%m-%d')
    fecha_fin_g2 = pd.to_datetime(fecha_fin_g2,format='%Y-%m-%d')

    temp_g2 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_g2) &
                                (temp_coctel_fuente['fecha_registro']<=fecha_fin_g2) &
                                (temp_coctel_fuente['lugar']==option_lugar_g2)]

    if not temp_g2.empty:
        temp_g2['semana'] = temp_g2['fecha_registro'].dt.isocalendar().year.map(str) +'-'+temp_g2['fecha_registro'].dt.isocalendar().week.map(str)
        temp_g2['a_favor'] = 0 # 1: a favor, 2: en contra
        temp_g2['a_favor'][temp_g2['id_posicion'].isin([1,2])] = 1
        temp_g2['en_contra'] = 0 # 1: a favor, 2: en contra
        temp_g2['en_contra'][temp_g2['id_posicion'].isin([4,5])] = 1

        if option_fuente_g2 == 'Radio':
            temp_g2 = temp_g2[temp_g2['id_fuente']==1].groupby('semana').agg({'id':'count','a_favor':'sum','en_contra':'sum','fecha_registro':'first'}).reset_index()
        elif option_fuente_g2 == 'TV':
            temp_g2 = temp_g2[temp_g2['id_fuente']==2].groupby('semana').agg({'id':'count','a_favor':'sum','en_contra':'sum','fecha_registro':'first'}).reset_index()
        elif option_fuente_g2 == 'Redes':
            temp_g2 = temp_g2[temp_g2['id_fuente']==3].groupby('semana').agg({'id':'count','a_favor':'sum','en_contra':'sum','fecha_registro':'first'}).reset_index()
        else:
            temp_g2 = temp_g2.groupby('semana').agg({'id':'count','a_favor':'sum','en_contra':'sum','fecha_registro':'first'}).reset_index()

        temp_g2 = temp_g2.rename(columns={'id':'Cantidad'})
        temp_g2 = temp_g2.sort_values('fecha_registro')
        temp_fecha = pd.DataFrame()
        temp_fecha['fecha'] = pd.date_range(start=fecha_inicio_g2, end=fecha_fin_g2)
        temp_fecha['semana'] = temp_fecha['fecha'].dt.isocalendar().year.map(str) +'-'+temp_fecha['fecha'].dt.isocalendar().week.map(str)
        temp_fecha = temp_fecha.groupby('semana').agg({'fecha':'first'}).reset_index()
        temp_fecha = temp_fecha.sort_values('fecha')
        del temp_fecha['fecha']
        temp_g2 = pd.merge(temp_fecha, temp_g2, how='left', on='semana')
        temp_g2['Cantidad'] = temp_g2['Cantidad'].fillna(0)
        temp_g2['a_favor'] = temp_g2['a_favor'].fillna(0)
        temp_g2['en_contra'] = temp_g2['en_contra'].fillna(0)
        temp_g2['a_favor'] = temp_g2['a_favor'] / temp_g2['Cantidad']
        temp_g2['en_contra'] = temp_g2['en_contra'] / temp_g2['Cantidad']
        temp_g2["a_favor"] = temp_g2["a_favor"] * 100
        temp_g2["en_contra"] = temp_g2["en_contra"] * 100
        temp_g2["a_favor"] = temp_g2["a_favor"].map('{:.2f}'.format)
        temp_g2["en_contra"] = temp_g2["en_contra"].map('{:.2f}'.format)

        st.write(f"Gráfico semanal de noticias a favor y en contra en {option_lugar_g2} entre {fecha_inicio_g2} y {fecha_fin_g2}")
        
        fig2 = go.Figure()
        fig2.add_trace(
                go.Scatter(x=temp_g2['semana'], y=temp_g2['a_favor'], mode='lines+markers', name='A favor', fillcolor = 'blue'))
        fig2.add_trace(
                go.Scatter(x=temp_g2['semana'], y=temp_g2['en_contra'], mode='lines+markers', name='En contra', line=dict(color='red'), marker=dict(color='red')))
        fig2.update_xaxes(type = "category",
                        title_text='Semana'
                        )
        fig2.update_yaxes(title_text='Porcentaje de noticias %')
        st.plotly_chart(fig2)

    else:
        st.warning("No hay datos para mostrar")
    #%% 4.- Grafico acumulativo porcentaje cocteles

    st.subheader("5.- Gráfico acumulativo porcentaje de cocteles en lugar y fecha específica")

    col1, col2, col3= st.columns(3)
    with col1:
        fecha_inicio_g3 = st.date_input(
        "Fecha Inicio g5",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g3 = st.date_input(
        "Fecha Fin g5",
        format="DD.MM.YYYY")
    with col3:
        option_fuente_g3 = st.selectbox(
        "Lugar g5",
        ("Radio", "TV", "Redes","Todos"))
    
    option_lugar_g3 = st.multiselect(
    "Lugar g5",
    lugares_uniques,lugares_uniques)

    fecha_inicio_g3 = pd.to_datetime(fecha_inicio_g3,format='%Y-%m-%d')
    fecha_fin_g3 = pd.to_datetime(fecha_fin_g3,format='%Y-%m-%d')

    temp_g3 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_g3) &
                                (temp_coctel_fuente['fecha_registro']<=fecha_fin_g3) &
                                (temp_coctel_fuente['lugar'].isin(option_lugar_g3))]

    if option_fuente_g3 == 'Radio':
        temp_g3 = temp_g3[temp_g3['id_fuente']==1]
    elif option_fuente_g3 == 'TV':
        temp_g3 = temp_g3[temp_g3['id_fuente']==2]
    elif option_fuente_g3 == 'Redes':
        temp_g3 = temp_g3[temp_g3['id_fuente']==3]

    if not temp_g3.empty:
        lista_lugares = list(temp_g3['lugar'].unique())
        temp_g3['semana'] = temp_g3['fecha_registro'].dt.isocalendar().year.map(str) +'-'+temp_g3['fecha_registro'].dt.isocalendar().week.map(lambda x: f"{x:02}")
        temp_g3 = temp_g3.groupby(['semana','lugar']).agg(coctel_mean = ("coctel", "mean")).reset_index()

        fig = px.line(temp_g3,
                    x="semana",
                    y="coctel_mean",
                    color='lugar',
                    title='Porcentaje de cocteles por semana %',
                    labels = {'semana':'Semana','coctel_mean':'Porcentaje de cocteles %'}
                    )
        fig.update_xaxes(type='category')
        st.plotly_chart(fig)

        # cuadro con el ultimo porcentaje semanal
        st.write(f"Porcentaje de cocteles por lugar en la última semana entre {fecha_inicio_g3} y {fecha_fin_g3} según {option_fuente_g3}")
        temp_g3 = temp_g3.sort_values('semana')
        temp_g3 = temp_g3.groupby('lugar').last().reset_index()
        temp_g3 = temp_g3[['lugar','coctel_mean']]
        temp_g3 = temp_g3.rename(columns={'coctel_mean':'pct_cocteles'})
        temp_g3['pct_cocteles'] = temp_g3['pct_cocteles'].map('{:.0%}'.format)
        st.dataframe(temp_g3, hide_index=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% #.- Top 3 mejores lugares segun fechas y fuente

    st.subheader("#.- Top 3 mejores porcentajes de coctel semanal por lugar en fuente y fecha específica")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_sn2 = st.date_input(
        "Fecha Inicio sn_2",
        format="DD.MM.YYYY")

    with col2:
        fecha_fin_sn2 = st.date_input(
        "Fecha Fin sn_2",
        format="DD.MM.YYYY")

    with col3:
        option_fuente_sn2 = st.selectbox(
        "Fuente sn_2",
        ("Radio", "TV", "Redes"))

    fecha_inicio_sn2 = pd.to_datetime(fecha_inicio_sn2,format='%Y-%m-%d')
    fecha_fin_sn2 = pd.to_datetime(fecha_fin_sn2,format='%Y-%m-%d')

    temp_sn2 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_sn2) &
                                    (temp_coctel_fuente['fecha_registro']<=fecha_fin_sn2)]

    if option_fuente_sn2 == 'Radio':
        temp_sn2 = temp_sn2[temp_sn2['id_fuente']==1]
    elif option_fuente_sn2 == 'TV':
        temp_sn2 = temp_sn2[temp_sn2['id_fuente']==2]
    elif option_fuente_sn2 == 'Redes':
        temp_sn2 = temp_sn2[temp_sn2['id_fuente']==3]

    if not temp_sn2.empty:
        temp_sn2["semana"] = temp_sn2['fecha_registro'].dt.isocalendar().year.map(str) +'-'+temp_sn2['fecha_registro'].dt.isocalendar().week.map(lambda x: f"{x:02}")
        temp_sn2 = temp_sn2.groupby(['lugar','semana']).agg({'coctel':'mean'}).reset_index()

        #we need create 1.- top 3 lugares with the highest percentage of coctel in the last week and 2.- bar chart of the top 3 places
        temp_sn2_last = temp_sn2.sort_values('semana')
        temp_sn2_last = temp_sn2.groupby(["lugar"]).last().reset_index()
        temp_sn2_last = temp_sn2_last.sort_values('coctel',ascending=False).head(3).reset_index(drop=True)
        temp_sn2_last["coctel"] = temp_sn2_last["coctel"] * 100
        temp_sn2_last["coctel"] = temp_sn2_last["coctel"].map('{:.2f}'.format)
        
        temp_sn2 = temp_sn2[temp_sn2['lugar'].isin(temp_sn2_last['lugar'])]
        temp_sn2["coctel"] = temp_sn2["coctel"] * 100

        fig_sn2 = px.line(temp_sn2,
                        x='semana',
                        y='coctel',
                        color='lugar',
                        title='Top 3 lugares con mayor porcentaje de cocteles',
                        labels = {'semana':'Semana','coctel':'Porcentaje de cocteles %'}
                        )
        
        fig_sn2.update_xaxes(type='category')
        st.write(f"Top 3 lugares con mayor porcentaje de cocteles en la última semana entre {fecha_inicio_sn2} y {fecha_fin_sn2} según {option_fuente_sn2}")    
        st.dataframe(temp_sn2_last, hide_index=True)
        st.plotly_chart(fig_sn2)

    else:
        st.warning("No hay datos para mostrar")



    #%% 5.- Top 3 mejores radios, redes, tv usar dataframes de programas y redes

    st.subheader("6.- Top 3 mejores radios, redes, tv en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g5 = st.date_input(
        "Fecha Inicio g6",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g5 = st.date_input(
        "Fecha Fin g6",
        format="DD.MM.YYYY")

    with col3:
        option_fuente_g5 = st.selectbox(
        "Fuente g6",
        ("Radio", "TV", "Redes"))

    with col4:
        option_lugar_g5 = st.selectbox(
        "Lugar g6",
        lugares_uniques)

    fecha_inicio_g5 = pd.to_datetime(fecha_inicio_g5,format='%Y-%m-%d')
    fecha_fin_g5 = pd.to_datetime(fecha_fin_g5,format='%Y-%m-%d')

    temp_g5_medio = temp_coctel_fuente_programas[(temp_coctel_fuente_programas['fecha_registro']>=fecha_inicio_g5) &
                                            (temp_coctel_fuente_programas['fecha_registro']<=fecha_fin_g5) &
                                            (temp_coctel_fuente_programas['lugar']==option_lugar_g5)]

    temp_g5_redes = temp_coctel_fuente_fb[(temp_coctel_fuente_fb['fecha_registro']>=fecha_inicio_g5) &
                                            (temp_coctel_fuente_fb['fecha_registro']<=fecha_fin_g5) &
                                            (temp_coctel_fuente_fb['lugar']==option_lugar_g5)]

    if option_fuente_g5 == 'Radio':
        temp_g5_medio = temp_g5_medio[temp_g5_medio['id_fuente']==1]
    elif option_fuente_g5 == 'TV':
        temp_g5_medio = temp_g5_medio[temp_g5_medio['id_fuente']==2]
    elif option_fuente_g5 == 'Redes':
        temp_g5_redes = temp_g5_redes[temp_g5_redes['id_fuente']==3]

    if option_fuente_g5 == "Redes" and not temp_g5_redes.empty:
        temp_g5_redes["semana"] = temp_g5_redes['fecha_registro'].dt.to_period('W').apply(lambda r: r.start_time + pd.Timedelta(days=4))
        temp_g5_redes_top = temp_g5_redes.groupby(['nombre_facebook_page']).agg({'coctel':'mean'}).reset_index()
        temp_g5_redes_top = temp_g5_redes_top.sort_values('coctel',ascending=False).head(3).reset_index(drop=True)
        st.write(temp_g5_redes_top)

        top_3_redes_list = temp_g5_redes_top['nombre_facebook_page'].tolist()

        temp_g5_redes = temp_g5_redes[temp_g5_redes['nombre_facebook_page'].isin(top_3_redes_list)]
        temp_g5_redes = temp_g5_redes.groupby(['semana','nombre_facebook_page']).agg({'coctel':'mean'}).reset_index()

        fig_5 = px.line(temp_g5_redes,
                        x='semana',
                        y='coctel',
                        color='nombre_facebook_page',
                        title='Top 3 redes sociales con mayor porcentaje de cocteles',
                        labels = {'semana':'Semana','coctel':'Porcentaje de cocteles %'}
                        )

        st.plotly_chart(fig_5)


    elif option_fuente_g5 != "Redes" and not temp_g5_medio.empty:
        temp_g5_medio["semana"] = temp_g5_medio['fecha_registro'].dt.to_period('W').apply(lambda r: r.start_time + pd.Timedelta(days=4))    
        temp_g5_medio_top = temp_g5_medio.groupby(['nombre_canal']).agg({'coctel':'mean'}).reset_index()
        temp_g5_medio_top = temp_g5_medio_top.sort_values('coctel',ascending=False).head(3).reset_index(drop=True)
        st.write(temp_g5_medio_top)

        top_3_medio_list = temp_g5_medio_top['nombre_canal'].tolist()

        temp_g5_medio = temp_g5_medio[temp_g5_medio['nombre_canal'].isin(top_3_medio_list)]
        temp_g5_medio = temp_g5_medio.groupby(['semana','nombre_canal']).agg({'coctel':'mean'}).reset_index()

        temp_g5_medio['semana'] = temp_g5_medio['semana'].map(str)
        fig_5 = px.line(temp_g5_medio,
                        x='semana',
                        y='coctel',
                        color='nombre_canal',
                        title='Top 3 medios con mayor porcentaje de cocteles',
                        labels = {'semana':'Semana','coctel':'Porcentaje de cocteles %'}
                        )
        
        st.plotly_chart(fig_5)

    else:
        st.warning("No hay datos para mostrar")
    #%% 6.- Crecimiento de cocteles por macroregion

    # Para radio y redes las macroregiones son: Macro región Sur 1 – Tacna, Puno y Cusco (radio y redes),
    #                                           Macro región Sur 2 – Ayacucho y Arequipa (radio y redes)
    #                                           Macro región Norte – Piura y Trujillo (radio y redes)
    #                                           Macro región Centro – Lima, Ica, Huánuco (radio y redes)
    #                                           Macro región Unacem – Lima Sur, Cañete, Tarma (radio y redes) 
    # Para tv la macroregion es: Macro región TV: Ayacucho, Piura y Arequipa


    macroregiones_radio_redes = ["Macro región Sur 1", "Macro región Sur 2", "Macro región Norte", "Macro región Centro", "Macro región UNACEM"]
    macroregiones_tv = ["Macro región TV"]

    macroregiones = {
        "Macro región Sur 1": ["Tacna", "Puno", "Cusco"],
        "Macro región Sur 2": ["Ayacucho", "Arequipa"],	
        "Macro región Norte": ["Piura", "Trujillo"],
        "Macro región Centro": ["Lima", "Ica", "Huanuco"],
        "Macro región UNACEM": ["Lima Sur", "Cañete", "Tarma"],
        "Macro región TV": ["Ayacucho", "Piura", "Arequipa"]
    }

    st.subheader("7.- Crecimiento de cocteles por macroregion en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g6 = st.date_input(
        "Fecha Inicio g7",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g6 = st.date_input(
        "Fecha Fin g7",
        format="DD.MM.YYYY")
    with col3:
        option_fuente_g6 = st.selectbox(
        "Fuente g7",
        ("Radio", "TV", "Redes"))
    with col4:
        if option_fuente_g6 in ["Radio", "Redes"]:
            option_macroregion_g6 = st.selectbox(
                "Macroregión g6",
                macroregiones_radio_redes)
        elif option_fuente_g6 == "TV":
            option_macroregion_g6 = st.selectbox(
                "Macroregión g6",
                macroregiones_tv)


    fecha_inicio_g6 = pd.to_datetime(fecha_inicio_g6,format='%Y-%m-%d')
    fecha_fin_g6 = pd.to_datetime(fecha_fin_g6,format='%Y-%m-%d')

    temp_g6 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_g6) &
                                    (temp_coctel_fuente['fecha_registro']<=fecha_fin_g6)]

    if option_fuente_g6 == 'Radio':
        temp_g6 = temp_g6[temp_g6['id_fuente']==1]
    elif option_fuente_g6 == 'TV':
        temp_g6 = temp_g6[temp_g6['id_fuente']==2]
    elif option_fuente_g6 == 'Redes':
        temp_g6 = temp_g6[temp_g6['id_fuente']==3]

    if not temp_g6.empty:
        departamentos = macroregiones.get(option_macroregion_g6, [])
        temp_g6 = temp_g6[temp_g6['lugar'].isin(departamentos)]
        temp_g6["semana"] = temp_g6['fecha_registro'].dt.to_period('W').apply(lambda r: r.start_time + pd.Timedelta(days=4))
        temp_g6 = temp_g6.groupby(['semana', 'lugar']).agg(coctel_mean = ("coctel", "mean")).reset_index()
        temp_g6["semana"] = temp_g6["semana"].dt.strftime('%Y-%m-%d')

        # a dos decimales

        temp_g6["coctel_mean"] = temp_g6["coctel_mean"] * 100 # a dos decimales

        fig_6 = go.Figure()

        # Iterar sobre cada lugar único y añadir una traza para cada uno
        for lugar in temp_g6['lugar'].unique():
            df_temp = temp_g6[temp_g6['lugar'] == lugar]
            fig_6.add_trace(go.Scatter(
                x=df_temp['semana'], 
                y=df_temp['coctel_mean'],
                mode='lines+markers',
                name=lugar,
                line=dict(width=2),
                marker=dict(size=6)
            ))

        fig_6.update_layout(
            title=f"Crecimiento de cocteles por macroregión en {option_macroregion_g6} entre {fecha_inicio_g6} y {fecha_fin_g6}",
            xaxis_title='Semana',
            yaxis_title='Crecimiento de Cocteles (%)',
            template='plotly_white',
            xaxis=dict(tickformat='%Y-%m-%d'))

        # Mostrar el gráfico
        st.plotly_chart(fig_6)
        st.write("Nota: Los valores muestran el porcentaje de cocteles en cada semana tomando como referencia el viernes")

    else:
        st.warning("No hay datos para mostrar")

    #%% 7.- Grafico de barras contando posiciones

    st.subheader("8.- Gráfico de barras contando posiciones en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g7 = st.date_input("Fecha Inicio g8",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g7 = st.date_input("Fecha Fin g8",
                                    format="DD.MM.YYYY")
    with col3:
        option_lugar_g7 = st.selectbox(
            "Lugar g8",
            lugares_uniques
            )
    with col4:
        option_nota_g7 = st.selectbox(
            "Nota g8",
            ("Con coctel", "Sin coctel", "Todos"))

    fecha_inicio_g7 = pd.to_datetime(fecha_inicio_g7, format='%Y-%m-%d')
    fecha_fin_g7 = pd.to_datetime(fecha_fin_g7, format='%Y-%m-%d')

    temp_g7 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g7) &
                                (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g7) &
                                (temp_coctel_fuente['lugar'] == option_lugar_g7)]

    if option_nota_g7 == 'Con coctel':
        temp_g7 = temp_g7[temp_g7['coctel'] == 1]
        titulo_g7 = f'Conteo de posiciones con coctel en {option_lugar_g7} por tipo de medio'
    elif option_nota_g7 == 'Sin coctel':
        temp_g7 = temp_g7[temp_g7['coctel'] == 0]
        titulo_g7 = f'Conteo de posiciones sin coctel en {option_lugar_g7} por tipo de medio'
    else:
        titulo_g7 = f'Conteo de posiciones en {option_lugar_g7} por tipo de medio'

    if not temp_g7.empty:
        temp_g7['semana'] = temp_g7['fecha_registro'].dt.isocalendar().year.map(str) +'-'+temp_g7['fecha_registro'].dt.isocalendar().week.map(str)
        conteo_total = temp_g7.groupby(['id_posicion', 'id_fuente']).size().reset_index(name='count')
        conteo_total['Posición'] = conteo_total['id_posicion'].map(id_posicion_dict)
        conteo_total['Tipo de Medio'] = conteo_total['id_fuente'].map(id_fuente_dict)
        conteo_total = conteo_total.dropna()

        fig_7 = px.bar(conteo_total,
                    x='Posición',
                    y='count',
                    color='Tipo de Medio',
                    title=titulo_g7,
                    barmode='group',
                    labels={'count': 'Conteo', 'Posición': 'Posición', 'Tipo de Medio': 'Tipo de Medio'},
                    color_discrete_map={'radio': '#c54b8c', 'tv': '#e4d00a', 'redes': '#8b9dce'},
                    text='count'
                    )

        fig_7.update_layout(xaxis_title='Posición',
                            yaxis_title='Conteo',
                            legend_title='Tipo de Medio')

        st.plotly_chart(fig_7)
        st.write(f"Gráfico de barras contando posiciones en {option_lugar_g7} entre {fecha_inicio_g7.strftime('%Y-%m-%d')} y {fecha_fin_g7.strftime('%Y-%m-%d')}")

    else:
        st.warning("No hay datos para mostrar")

    #%% 8.- grafico de dona que representa el porcentaje de posiciones

    st.subheader("9.- Gráfico de dona que representa el porcentaje de posiciones en lugar y fecha específica")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fecha_inicio_g8 = st.date_input("Fecha Inicio g9",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g8 = st.date_input("Fecha Fin g9",
                                    format="DD.MM.YYYY")
    with col3:
        option_fuente_g8 = st.selectbox(
        "Fuente g9",
        ("Radio", "TV", "Redes","Todos"))
    with col4:
        option_nota_g8 = st.selectbox(
        "Nota g9",
        ("Con coctel", "Sin coctel", "Todos"))

    option_lugar_g8 = st.multiselect(
    "Lugar g9",
    lugares_uniques,lugares_uniques)

    fecha_inicio_g8 = pd.to_datetime(fecha_inicio_g8, format='%Y-%m-%d')
    fecha_fin_g8 = pd.to_datetime(fecha_fin_g8, format='%Y-%m-%d')

    temp_g8 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g8) &
                                (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g8) & 
                                (temp_coctel_fuente['lugar'].isin(option_lugar_g8))]

    if option_nota_g8 == 'Con coctel':
        temp_g8 = temp_g8[temp_g8['coctel']==1]
        titulo_g8 = 'Porcentaje de posiciones con coctel respecto del total'
    elif option_nota_g8 == 'Sin coctel':
        temp_g8 = temp_g8[temp_g8['coctel']==0]
        titulo_g8 = 'Porcentaje de posiciones sin coctel respecto del total'
    else:
        titulo_g8 = 'Porcentaje de posiciones respecto del total'

    if option_fuente_g8 == 'Radio':
        temp_g8 = temp_g8[temp_g8['id_fuente']==1]
    elif option_fuente_g8 == 'TV':
        temp_g8 = temp_g8[temp_g8['id_fuente']==2]
    elif option_fuente_g8 == 'Redes':
        temp_g8 = temp_g8[temp_g8['id_fuente']==3]

    if not temp_g8.empty:
        conteo_total_g8 = temp_g8.groupby(['id_posicion']).size().reset_index(name='count')

        conteo_total_g8['Posición'] = conteo_total_g8['id_posicion'].map(id_posicion_dict)
        conteo_total_g8 = conteo_total_g8.dropna()

        #porcentajes
        conteo_total_g8['Porcentaje'] = conteo_total_g8['count'] / conteo_total_g8['count'].sum()
        conteo_total_g8['Porcentaje'] = conteo_total_g8['Porcentaje'].map('{:.0%}'.format)

        fig_8 = px.pie(conteo_total_g8,
                    values='count',
                    names='Posición',
                    title=titulo_g8,
                    color='Posición',
                    color_discrete_map=color_posicion_dict,
                    hole=0.3
                    )
        st.plotly_chart(fig_8)
        st.write(f"Gráfico de dona que representa el porcentaje de posiciones en {', '.join(option_lugar_g8)} entre {fecha_inicio_g8.strftime('%Y-%m-%d')} y {fecha_fin_g8.strftime('%Y-%m-%d')}")

    else:
        st.warning("No hay datos para mostrar")

    #%% 9.- porcentaje de acontecimientos con coctel

    st.subheader("10.- Porcentaje de acontecimientos con coctel en lugar y fecha específica")

    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_inicio_g9 = st.date_input("Fecha Inicio g10",
                                        format="DD.MM.YYYY")

    with col2:
        fecha_fin_g9 = st.date_input("Fecha Fin g10",
                                    format="DD.MM.YYYY")
        
    with col3:
        option_fuente_g9 = st.selectbox(
        "Fuente g10",
        ("Radio", "TV", "Redes","Todos"))

    option_lugar_g9 = st.multiselect(
    "Lugar g10",
    lugares_uniques,lugares_uniques)

    fecha_inicio_g9 = pd.to_datetime(fecha_inicio_g9, format='%Y-%m-%d')
    fecha_fin_g9 = pd.to_datetime(fecha_fin_g9, format='%Y-%m-%d')

    temp_g9 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g9) & (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g9) & (temp_coctel_fuente['lugar'].isin(option_lugar_g9))]

    if option_fuente_g9 == 'Radio':
        temp_g9 = temp_g9[temp_g9['id_fuente']==1]
    elif option_fuente_g9 == 'TV':
        temp_g9 = temp_g9[temp_g9['id_fuente']==2]
    elif option_fuente_g9 == 'Redes':
        temp_g9 = temp_g9[temp_g9['id_fuente']==3]

    if not temp_g9.empty:
        conteo_total_g9 = temp_g9.groupby(['coctel']).size().reset_index(name='count')
        conteo_total_g9['Coctel'] = conteo_total_g9['coctel'].map(coctel_dict)
        conteo_total_g9['Porcentaje'] = conteo_total_g9['count'] / conteo_total_g9['count'].sum()
        conteo_total_g9['Porcentaje'] = conteo_total_g9['Porcentaje'].map('{:.0%}'.format)
        st.write(f"Porcentaje de acontecimientos con coctel en {option_lugar_g9} entre {fecha_inicio_g9} y {fecha_fin_g9}")
        fig_9 = px.pie(conteo_total_g9,
                    values='count',
                    names='Coctel',
                    title='Porcentaje de acontecimientos con coctel',
                    hole=0.3,
                    color='Coctel',
                    color_discrete_map={'Sin coctel': 'orange', 'Con coctel': 'Blue'}
                    )

        st.plotly_chart(fig_9)

    else:
        st.warning("No hay datos para mostrar")

    #%% 12.- Tabla que muestra la cantidad de cocteles por fuente y lugar

    st.subheader("11.- Cantidad de cocteles por fuente y lugar en fecha específica")

    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio_g12 = st.date_input("Fecha Inicio g11",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g12 = st.date_input("Fecha Fin g11",
                                    format="DD.MM.YYYY")

    option_lugar_g12 = st.multiselect("Lugar g11",
                                    lugares_uniques,
                                    lugares_uniques)

    fecha_inicio_g12 = pd.to_datetime(fecha_inicio_g12, format='%Y-%m-%d')
    fecha_fin_g12 = pd.to_datetime(fecha_fin_g12, format='%Y-%m-%d')

    temp_g12 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g12) & (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g12) & (temp_coctel_fuente['lugar'].isin(option_lugar_g12))]

    if not temp_g12.empty:
        conteo_total_g12 = temp_g12.groupby(['id_fuente', 'lugar', 'coctel']).size().reset_index(name='count')
        conteo_total_g12 = conteo_total_g12[conteo_total_g12['coctel'] == 1]
        conteo_total_g12['Fuente'] = conteo_total_g12['id_fuente'].map(id_fuente_dict)

        conteo_total_g12 = pd.crosstab(conteo_total_g12['lugar'],
                                    conteo_total_g12['Fuente'],
                                    values=conteo_total_g12['count'],
                                    aggfunc='sum').fillna(0).reset_index()
        conteo_total_g12 = conteo_total_g12.rename(columns={"tv": "televisión"})
        st.write(f"Cantidad de cocteles por fuente y lugar en {option_lugar_g12} entre {fecha_inicio_g12} y {fecha_fin_g12}")
        st.dataframe(conteo_total_g12, hide_index=True, width=300)

    else:
        st.warning("No hay datos para mostrar")

    #%% 13.- Reporte quincenal acerca de cuantas radios, redes y tv generaron coctel
    st.subheader("12.- Reporte semanal acerca de cuantas radios, redes y tv generaron coctel")

    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio_g13 = st.date_input("Fecha Inicio g12",
                                        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g13 = st.date_input("Fecha Fin g12",
                                    format="DD.MM.YYYY")
        
    option_lugar_g13 = st.multiselect("Lugar g12",
                                    lugares_uniques,
                                    lugares_uniques)

    fecha_inicio_g13 = pd.to_datetime(fecha_inicio_g13, format='%Y-%m-%d')
    fecha_fin_g13 = pd.to_datetime(fecha_fin_g13, format='%Y-%m-%d')

    temp_g13 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g13) &
                                (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g13) &
                                (temp_coctel_fuente['lugar'].isin(option_lugar_g13))]

    temp_g13_fb = temp_coctel_fuente_fb[(temp_coctel_fuente_fb['fecha_registro'] >= fecha_inicio_g13) &
                                        (temp_coctel_fuente_fb['fecha_registro'] <= fecha_fin_g13) &
                                        (temp_coctel_fuente_fb['lugar'].isin(option_lugar_g13))]

    if not temp_g13.empty:
        temp_g13["semana"] = temp_g13['fecha_registro'].dt.isocalendar().year.map(str) +'-'+temp_g13['fecha_registro'].dt.isocalendar().week.map(str)

        temp_merged = pd.merge(temp_g13, temp_g13_fb[['fecha_registro', 'acontecimiento', 'coctel','id_fuente', 'lugar', 'nombre_facebook_page']], on=['fecha_registro', 'acontecimiento', 'coctel','id_fuente', 'lugar'], how='left')
        temp_merged['id_canal'] = temp_merged['id_canal'].fillna(temp_merged['nombre_facebook_page'])
        
        temp_g13_coctel = temp_merged[temp_merged['coctel'] == 1] #solo coctel

        conteo_total_g13 = temp_g13_coctel.groupby(['id_fuente', 'lugar', 'id_canal','semana']).size().reset_index(name='count')

        #mapeo de fuente
        conteo_total_g13['Fuente'] = conteo_total_g13['id_fuente'].astype(int).map(id_fuente_dict)

        conteo_canal_g13 = conteo_total_g13.groupby(['Fuente', 'lugar'])['id_canal'].nunique().reset_index(name='conteo_canal')

        conteo_canal_g13 = pd.crosstab(conteo_canal_g13['lugar'],
                                    conteo_canal_g13['Fuente'],
                                    values=conteo_canal_g13['conteo_canal'],
                                    aggfunc='sum').fillna(0).reset_index()


        st.dataframe(conteo_canal_g13, hide_index=True, width=300)

    else:
        st.warning("No hay datos para mostrar")

    #%% 18.- Se realiza un conteo mensual acerca de la cantidad de coctel utilizado por región, dividido en redes, radio y tv. Además se muestra el combinado de todas las regiones
    st.subheader("13.- Conteo mensual de la cantidad de coctel utilizado por región, dividido en redes, radio y tv")

    col1, col2 = st.columns(2)

    with col1:
        year_inicio_g18 = st.selectbox("Año Inicio g13", list(range(2023, 2025)), index=0)
        month_inicio_g18 = st.selectbox("Mes Inicio g13", list(range(1, 13)), index=0)

    with col2:
        year_fin_g18 = st.selectbox("Año Fin g13", list(range(2023, 2025)), index=1)
        month_fin_g18 = st.selectbox("Mes Fin g13", list(range(1, 13)), index=11)


    option_lugar_g18 = st.multiselect("Lugar g13",
                                    lugares_uniques,
                                    lugares_uniques)

    fecha_inicio_g18 = pd.to_datetime(f'{year_inicio_g18}-{month_inicio_g18}-01')
    fecha_fin_g18 = pd.to_datetime(f'{year_fin_g18}-{month_fin_g18}-01') + pd.offsets.MonthEnd(1)

    temp_g18 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g18) &
                                    (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g18) &
                                    (temp_coctel_fuente['lugar'].isin(option_lugar_g18))]

    non_numeric_coctel = temp_g18['coctel'].apply(lambda x: isinstance(x, str) or not pd.api.types.is_number(x))
    temp_g18.loc[non_numeric_coctel, 'coctel'] = 1.0

    if not temp_g18.empty:
        temp_g18["mes"] = temp_g18['fecha_registro'].dt.month
        temp_g18["año"] = temp_g18['fecha_registro'].dt.year

        temp_g18 = temp_g18[["fecha_registro", "acontecimiento", "lugar", "id_fuente", "coctel", "mes", "año"]]

        temp_g18["Fuente"] = temp_g18["id_fuente"].map(id_fuente_dict)

        temp_g18 = temp_g18.dropna().drop_duplicates().reset_index(drop=True)

        temp_g18["año_mes"] = temp_g18["año"].astype(str) + "-" + temp_g18["mes"].astype(str)

        # Agrupar los datos por lugar, año_mes, y Fuente para contar el total de cocteles
        conteo_cocteles_lugar = temp_g18.groupby(['lugar', 'año_mes', 'Fuente']).agg({'coctel': 'sum'}).reset_index()

        st.write(f"Conteo mensual de la cantidad de coctel utilizado por región, dividido en redes, radio y tv en {option_lugar_g18} entre {fecha_inicio_g18} y {fecha_fin_g18}")
        st.dataframe(conteo_cocteles_lugar, hide_index=True, width=300)

        conteo_cocteles_mes = temp_g18.groupby(['año_mes', 'Fuente']).agg({'coctel': 'sum'}).reset_index()

        #graficando el conteo de cocteles por mes y fuente

        fig_18 = px.bar(conteo_cocteles_mes,
                        x='año_mes',
                        y='coctel',
                        color='Fuente',
                        barmode='stack',
                        title='Conteo de cocteles por mes y fuente',
                        labels={'año_mes': 'Año y Mes',
                                'coctel': 'Número de Cocteles',
                                'Fuente': 'Fuente'},
                        text='coctel', 
                        color_discrete_map = {'radio': '#c54b8c', 'tv': '#e4d00a', 'redes': '#8b9dce'}
                        )

        st.plotly_chart(fig_18)

    else:
        st.warning("No hay datos para mostrar")

    #%% 20.- Se tiene cuadros divididos por radio y redes en el cual se muestran las notas en general que sean a favor ( a favor y mayormente a favor), neutral y en contra (en contra y mayormente en contra)
    st.subheader("14.- Porcentaje de notas que sean a favor, neutral y en contra")

    col1, col2, col3 = st.columns(3)

    with col1:
        fecha_inicio_g20 = st.date_input("Fecha Inicio g14",
                                        format="DD.MM.YYYY")

    with col2:
        fecha_fin_g20 = st.date_input("Fecha Fin g14",
                                    format="DD.MM.YYYY")
    with col3:
        option_nota_g20 = st.selectbox(
        "Notas g14",
        ("Con coctel", "Sin coctel", "Todos"))


    option_lugar_g20 = st.multiselect("Lugar g14",
                                    lugares_uniques,
                                    lugares_uniques)

    fecha_inicio_g20 = pd.to_datetime(fecha_inicio_g20, format='%Y-%m-%d')
    fecha_fin_g20 = pd.to_datetime(fecha_fin_g20, format='%Y-%m-%d')

    temp_g20 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro'] >= fecha_inicio_g20) &
                                    (temp_coctel_fuente['fecha_registro'] <= fecha_fin_g20) &
                                    (temp_coctel_fuente['lugar'].isin(option_lugar_g20))].dropna()

    if option_nota_g20 == "Con coctel":
        temp_g20 = temp_g20[temp_g20['coctel'] == 1]
        titulo_g20 = "Porcentaje de notas que sean a favor, neutral y en contra con coctel"
    elif option_nota_g20 == "Sin coctel":
        temp_g20 = temp_g20[temp_g20['coctel'] == 0]
        titulo_g20 = "Porcentaje de notas que sean a favor, neutral y en contra sin coctel" 
    else:
        titulo_g20 = "Porcentaje de notas que sean a favor, neutral y en contra"

    if not temp_g20.empty:

        temp_g20["mes"] = temp_g20['fecha_registro'].dt.month
        temp_g20["año"] = temp_g20['fecha_registro'].dt.year
        temp_g20["año_mes"] = temp_g20["año"].astype(str) + "-" + temp_g20["mes"].astype(str)

        temp_g20["a_favor"] = 0
        temp_g20["en_contra"] = 0
        temp_g20["neutral"] = 0


        temp_g20.loc[temp_g20['id_posicion'].isin([1, 2]), "a_favor"] = 1
        temp_g20.loc[temp_g20['id_posicion'].isin([4, 5]), "en_contra"] = 1
        temp_g20.loc[temp_g20['id_posicion'] == 3, "neutral"] = 1


        temp_g20 = temp_g20[["año_mes", "fecha_registro", "acontecimiento", "lugar", "a_favor", "en_contra", "neutral"]]


        conteo_notas_20 = temp_g20.groupby('año_mes').agg({'a_favor': 'sum',
                                                            'en_contra': 'sum',
                                                            'neutral': 'sum'}).reset_index()

        st.write(f"Porcentaje de notas que sean a favor, neutral y en contra en {option_lugar_g20} entre {fecha_inicio_g20.strftime('%d-%m-%Y')} y {fecha_fin_g20.strftime('%d-%m-%Y')}")

        conteo_notas_20_pct = conteo_notas_20.copy()

        conteo_notas_20_pct["total"] = conteo_notas_20_pct["a_favor"] + conteo_notas_20_pct["en_contra"] + conteo_notas_20_pct["neutral"]

        #2 decimales
        conteo_notas_20_pct["a_favor_pct"] = (conteo_notas_20_pct['a_favor'] / conteo_notas_20_pct['total']) * 100 
        conteo_notas_20_pct["en_contra_pct"] = (conteo_notas_20_pct['en_contra'] / conteo_notas_20_pct['total']) * 100
        conteo_notas_20_pct["neutral_pct"] = (conteo_notas_20_pct['neutral'] / conteo_notas_20_pct['total']) * 100

        conteo_notas_20_pct = conteo_notas_20_pct[["año_mes", "a_favor_pct", "en_contra_pct", "neutral_pct"]]
        fig_20 = px.bar(conteo_notas_20_pct,
                    x='año_mes',
                    y=['a_favor_pct', 'en_contra_pct', 'neutral_pct'],
                    barmode='stack',
                    title=titulo_g20,
                    labels={'año_mes': 'Año y Mes',
                            'value': 'Porcentaje',
                            'variable': 'Tipo de Nota'},
                    text_auto=True,
                    color_discrete_map={'a_favor_pct': 'blue',
                                        'en_contra_pct': 'red',
                                        'neutral_pct': 'gray'}
                    )
        fig_20.update_layout(barmode='stack', xaxis={'categoryorder':'category ascending'})
        fig_20.for_each_trace(lambda t: t.update(name=t.name.replace('_pct', ' (%)')))
        conteo_notas_20_pct["a_favor_pct"] = conteo_notas_20_pct["a_favor_pct"].map("{:.2f}".format)
        conteo_notas_20_pct["en_contra_pct"] = conteo_notas_20_pct["en_contra_pct"].map("{:.2f}".format)
        conteo_notas_20_pct["neutral_pct"] = conteo_notas_20_pct["neutral_pct"].map("{:.2f}".format)
        conteo_notas_20_pct = conteo_notas_20_pct.rename(columns={"a_favor_pct": "A favor (%)", "en_contra_pct": "En contra (%)", "neutral_pct": "Neutral (%)"})

        st.dataframe(conteo_notas_20_pct, hide_index=True)

        st.plotly_chart(fig_20)
        st.write("Los porcentajes se calcularon sobre el total de notas considerando coctel y otras fuentes")
    else:
        st.warning("No hay datos para mostrar")

    #%% 22.- Se realizan gráficos de tendencia de los mensajes emitidos por radio o redes por alguna región, por ejemplo:
    st.subheader("15.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específica")

    col1, col2 = st.columns(2) 
    with col1:
        fecha_inicio_g22 = st.date_input(
        "Fecha Inicio g15",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g22 = st.date_input(
        "Fecha Fin g15",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)
    with col1:
        option_fuente_g22 = st.selectbox(
        "Fuente g15",
        ("Radio", "TV", "Redes","Todos"),)
    with col2:
        option_lugar_g22 = st.selectbox(
        "Lugar g15",
        lugares_uniques)
    with col3:
        option_nota_g22 = st.selectbox(
        "Nota g15",
        ("Con coctel", "Sin coctel", "Todos")
        )

    fecha_inicio_g22 = pd.to_datetime(fecha_inicio_g22,format='%Y-%m-%d')
    fecha_fin_g22 = pd.to_datetime(fecha_fin_g22,format='%Y-%m-%d')

    temp_g22 = temp_coctel_fuente[(temp_coctel_fuente['fecha_registro']>=fecha_inicio_g22) & 
                                (temp_coctel_fuente['fecha_registro']<=fecha_fin_g22) &
                                (temp_coctel_fuente['lugar']==option_lugar_g22)]

    if option_nota_g22 == 'Con coctel':
        temp_g22 = temp_g22[temp_g22['coctel'] == 1]
        titulo = f"Proporción de mensajes emitidos con coctel por {option_fuente_g22} en {option_lugar_g22} entre {fecha_inicio_g22.strftime('%d-%m-%Y')} y {fecha_fin_g22.strftime('%d-%m-%Y')}"
    elif option_nota_g22 == 'Sin coctel':
        temp_g22 = temp_g22[temp_g22['coctel'] == 0]
        titulo = f"Proporción de mensajes emitidos sin coctel por {option_fuente_g22} en {option_lugar_g22} entre {fecha_inicio_g22.strftime('%d-%m-%Y')} y {fecha_fin_g22.strftime('%d-%m-%Y')}"
    else:
        titulo = f"Proporción de mensajes emitidos por {option_fuente_g22} en {option_lugar_g22} entre {fecha_inicio_g22.strftime('%d-%m-%Y')} y {fecha_fin_g22.strftime('%d-%m-%Y')}"

    if option_fuente_g22 == 'Radio':
        temp_g22 = temp_g22[temp_g22['id_fuente']==1].groupby('color')["id"].count().reset_index()
    elif option_fuente_g22 == 'TV':
        temp_g22 = temp_g22[temp_g22['id_fuente']==2].groupby('color')["id"].count().reset_index()
    elif option_fuente_g22 == 'Redes':
        temp_g22 = temp_g22[temp_g22['id_fuente']==3].groupby('color')["id"].count().reset_index()
    else:
        temp_g22 = temp_g22.groupby('color')["id"].count().reset_index()

    #reanombrar columnas id por frecuencia y agregar columna porcentaje

    temp_g22 = temp_g22.rename(columns={'id':'frecuencia'})
    temp_g22['porcentaje'] = temp_g22['frecuencia']/temp_g22['frecuencia'].sum()
    temp_g22['porcentaje'] = temp_g22['porcentaje'].apply(lambda x:"{:.2%}".format(x))
    temp_g22 = temp_g22.reset_index(drop=True)

    if not temp_g22.empty:

        fig_22 = px.pie(temp_g22,
                        values='frecuencia',
                        names='color',
                        title=titulo,
                        width=600,
                        hole=0.3,
                        color='color',
                        color_discrete_map=color_discrete_map,
                        )
        st.plotly_chart(fig_22, use_container_width=True)
        st.dataframe(temp_g22, hide_index=True)
    else:
        st.warning("No hay datos para mostrar")

    #%% 23.- Se realizan gráficas de los mensajes emitidos por tema y en estos también se subdividen si son de tipo neutral, a favor, en contra, etc.
    st.subheader("16.- Recuento de Mensajes Emitidos por Tema y Tipo de Nota en Lugar y Fecha Específica")
    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio_g23 = st.date_input(
        "Fecha Inicio g16",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g23 = st.date_input(
        "Fecha Fin g16",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)
    with col1:
        option_fuente_g23 = st.selectbox(
        "Fuente g16",
        ("Radio", "TV", "Redes","Todos"))
    with col2:
        option_lugar_g23 = st.selectbox(
        "Lugar g16",
        lugares_uniques)
    with col3:
        option_nota_g23 = st.selectbox(
        "Nota g16",
        ("Con coctel", "Sin coctel", "Todos")
        )

    fecha_inicio_g23 = pd.to_datetime(fecha_inicio_g23,format='%Y-%m-%d')
    fecha_fin_g23 = pd.to_datetime(fecha_fin_g23,format='%Y-%m-%d')

    temp_g23 = temp_coctel_temas[(temp_coctel_temas['fecha_registro']>=fecha_inicio_g23) &
                                    (temp_coctel_temas['fecha_registro']<=fecha_fin_g23) &
                                    (temp_coctel_temas['lugar']==option_lugar_g23)]

    if option_nota_g23 == 'Con coctel':
        temp_g23 = temp_g23[temp_g23['coctel'] == 1]
        titulo = f"Recuento de mensajes emitidos con coctel por {option_fuente_g23} en {option_lugar_g23} entre {fecha_inicio_g23.strftime('%d-%m-%Y')} y {fecha_fin_g23.strftime('%d-%m-%Y')}"
    elif option_nota_g23 == 'Sin coctel':
        temp_g23 = temp_g23[temp_g23['coctel'] == 0]
        titulo = f"Recuento de mensajes emitidos sin coctel por {option_fuente_g23} en {option_lugar_g23} entre {fecha_inicio_g23.strftime('%d-%m-%Y')} y {fecha_fin_g23.strftime('%d-%m-%Y')}"
    else:
        titulo = f"Recuento de mensajes emitidos por {option_fuente_g23} en {option_lugar_g23} entre {fecha_inicio_g23.strftime('%d-%m-%Y')} y {fecha_fin_g23.strftime('%d-%m-%Y')}"

    if option_fuente_g23 == 'Radio':
        temp_g23 = temp_g23[temp_g23['id_fuente']==1]
    elif option_fuente_g23 == 'TV':
        temp_g23 = temp_g23[temp_g23['id_fuente']==2]
    elif option_fuente_g23 == 'Redes':
        temp_g23 = temp_g23[temp_g23['id_fuente']==3]

    if not temp_g23.empty:
        temp_g23["id_posicion"] = temp_g23["id_posicion"].map(id_posicion_dict)

        df_grouped = temp_g23.groupby(['descripcion', 'id_posicion']).size().reset_index(name='frecuencia')

        top_10_temas = df_grouped.groupby('descripcion')['frecuencia'].sum().nlargest(10).index
        df_top_10 = df_grouped[df_grouped['descripcion'].isin(top_10_temas)]

        fig_23 = px.bar(
            df_top_10,
            x='descripcion',
            y='frecuencia',
            title=titulo,
            color='id_posicion',
            text='frecuencia',
            barmode='stack',
            labels={'frecuencia': 'Frecuencia', 'descripcion': 'Tema', 'id_posicion': 'Posición'},
            category_orders={'descripcion': top_10_temas},
            color_discrete_map=color_posicion_dict,
            height=500
        )

        st.plotly_chart(fig_23, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")


    #%% 24.- Proporción de mensajes emitidos por tema en lugar y fecha específica
    st.subheader("17.- Proporción de Mensajes Emitidos por Fuente y Tipo de Nota en un Lugar y Fecha Específicos")

    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio_g24 = st.date_input(
        "Fecha Inicio g17",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g24 = st.date_input(
        "Fecha Fin g17",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)    
    with col1:
        option_fuente_g24 = st.selectbox(
        "Fuente g17",
        ("Radio", "TV", "Redes","Todos"),)

    with col2:
        option_lugar_g24 = st.selectbox(
        "Lugar g17",
        lugares_uniques)

    with col3:
        option_nota_g24 = st.selectbox(
        "Nota g17",
        ("Con coctel", "Sin coctel", "Todos")
        )

    fecha_inicio_g24 = pd.to_datetime(fecha_inicio_g24,format='%Y-%m-%d')
    fecha_fin_g24 = pd.to_datetime(fecha_fin_g24,format='%Y-%m-%d')

    temp_g24 = temp_coctel_temas[(temp_coctel_temas['fecha_registro']>=fecha_inicio_g24) &
                                    (temp_coctel_temas['fecha_registro']<=fecha_fin_g24) &
                                    (temp_coctel_temas['lugar']==option_lugar_g24)]

    if option_nota_g24 == 'Con coctel':
        temp_g24 = temp_g24[temp_g24['coctel'] == 1]
        titulo = f"Proporción de mensajes emitidos con coctel por {option_fuente_g24} en {option_lugar_g24} entre {fecha_inicio_g24.strftime('%d-%m-%Y')} y {fecha_fin_g24.strftime('%d-%m-%Y')}"
    elif option_nota_g24 == 'Sin coctel':
        temp_g24 = temp_g24[temp_g24['coctel'] == 0]
        titulo = f"Proporción de mensajes emitidos sin coctel por {option_fuente_g24} en {option_lugar_g24} entre {fecha_inicio_g24.strftime('%d-%m-%Y')} y {fecha_fin_g24.strftime('%d-%m-%Y')}"
    else:
        titulo = f"Proporción de mensajes emitidos por {option_fuente_g24} en {option_lugar_g24} entre {fecha_inicio_g24.strftime('%d-%m-%Y')} y {fecha_fin_g24.strftime('%d-%m-%Y')}"

    if option_fuente_g24 == 'Radio':
        temp_g24 = temp_g24[temp_g24['id_fuente']==1]
    elif option_fuente_g24 == 'TV':
        temp_g24 = temp_g24[temp_g24['id_fuente']==2]
    elif option_fuente_g24 == 'Redes':
        temp_g24 = temp_g24[temp_g23['id_fuente']==3]

    # solo agfrupar por top 10 temas y porcentajes
    if not temp_g24.empty:

        df_grouped_24 = temp_g24.groupby(['descripcion']).size().reset_index(name='frecuencia')
        top_10_temas_24 = df_grouped_24.nlargest(10, 'frecuencia')['descripcion']
        df_top_10_24 = df_grouped_24[df_grouped_24['descripcion'].isin(top_10_temas_24)]

        df_top_10_24['porcentaje'] = df_top_10_24['frecuencia']/df_grouped_24['frecuencia'].sum()
        df_top_10_24["porcentaje"] = df_top_10_24["porcentaje"]*100
        df_top_10_24['porcentaje'] = df_top_10_24['porcentaje'].apply(lambda x:"{:.2f}".format(x))

        fig_24 = px.bar(df_top_10_24,
                        x="porcentaje",
                        y="descripcion",
                        title=titulo,
                        orientation='h',
                        text="porcentaje", 
                        labels={'porcentaje': 'Porcentaje %', 'descripcion': 'Temas'}
                        )

        fig_24.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_24, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% 25.- Se busca conocer la tendencia de las notas emitidas ya sean de coctel o no o combinadas por radio o redes
    st.subheader("18.- Tendencia de las notas emitidas en lugar y fecha específica por fuente y tipo de nota")

    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio_g25 = st.date_input(
        "Fecha Inicio g18",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g25 = st.date_input(
        "Fecha Fin g18",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)

    with col1:
        option_fuente_g25 = st.selectbox(
        "Fuente g18",
        ("Radio", "TV", "Redes"),)

    with col2:
        option_lugar_g25 = st.selectbox(
        "Lugar g18",
        lugares_uniques)

    with col3:
        option_nota_g25 = st.selectbox(
        "Nota g18",
        ("Con coctel", "Sin coctel", "Todos")
        )


    fecha_inicio_g25 = pd.to_datetime(fecha_inicio_g25,format='%Y-%m-%d')
    fecha_fin_g25 = pd.to_datetime(fecha_fin_g25,format='%Y-%m-%d')

    temp_g25_medio = temp_coctel_fuente_programas[(temp_coctel_fuente_programas['fecha_registro']>=fecha_inicio_g25) &
                                                (temp_coctel_fuente_programas['fecha_registro']<=fecha_fin_g25) &
                                                (temp_coctel_fuente_programas['lugar']==option_lugar_g25)]

    temp_g25_redes = temp_coctel_fuente_fb[(temp_coctel_fuente_fb['fecha_registro']>=fecha_inicio_g25) &
                                            (temp_coctel_fuente_fb['fecha_registro']<=fecha_fin_g25) &
                                            (temp_coctel_fuente_fb['lugar']==option_lugar_g25)]

    if option_nota_g25 == 'Con coctel':
        temp_g25_medio = temp_g25_medio[temp_g25_medio['coctel'] == 1]
        temp_g25_redes = temp_g25_redes[temp_g25_redes['coctel'] == 1]
        titulo = f"Tendencia de las notas emitidas con coctel por {option_fuente_g25} en {option_lugar_g25} entre {fecha_inicio_g25.strftime('%d-%m-%Y')} y {fecha_fin_g25.strftime('%d-%m-%Y')}"
    elif option_nota_g25 == 'Sin coctel':
        temp_g25_medio = temp_g25_medio[temp_g25_medio['coctel'] == 0]
        temp_g25_redes = temp_g25_redes[temp_g25_redes['coctel'] == 0]
        titulo = f"Tendencia de las notas emitidas sin coctel por {option_fuente_g25} en {option_lugar_g25} entre {fecha_inicio_g25.strftime('%d-%m-%Y')} y {fecha_fin_g25.strftime('%d-%m-%Y')}"
    else:
        titulo = f"Tendencia de las notas emitidas por {option_fuente_g25} en {option_lugar_g25} entre {fecha_inicio_g25.strftime('%d-%m-%Y')} y {fecha_fin_g25.strftime('%d-%m-%Y')}"

    if option_fuente_g25 == 'Radio':
        temp_g25_medio = temp_g25_medio[temp_g25_medio['id_fuente']==1]
    elif option_fuente_g25 == 'TV':
        temp_g25_medio = temp_g25_medio[temp_g25_medio['id_fuente']==2]
    elif option_fuente_g25 == 'Redes':
        temp_g25_redes = temp_g25_redes[temp_g25_redes['id_fuente']==3]


    if option_fuente_g25 == "Redes" and not temp_g25_redes.empty:
        df_grouped_25_redes = temp_g25_redes.groupby(['nombre_facebook_page', 'id_posicion']).size().reset_index(name='frecuencia')
        df_grouped_25_redes = df_grouped_25_redes.sort_values(by='frecuencia', ascending=False)
        df_grouped_25_redes['id_posicion'] = df_grouped_25_redes['id_posicion'].map(id_posicion_dict)
        
        fig_25_redes = px.bar(df_grouped_25_redes,
                            x='nombre_facebook_page',
                            y='frecuencia',
                            title = titulo, 
                            color='id_posicion',
                            barmode='stack',
                            labels={'frecuencia': 'Frecuencia', 'nombre_facebook_page': 'Pagina Facebook', 'id_posicion': 'Posición'},
                            color_discrete_map=color_posicion_dict,
                            text = 'frecuencia'
                            )
        fig_25_redes.update_layout(height=600)
        st.plotly_chart(fig_25_redes, use_container_width=True)

    elif option_fuente_g25 != "Redes" and not temp_g25_medio.empty:
        df_grouped_25_medio = temp_g25_medio.groupby(['nombre_canal', 'id_posicion']).size().reset_index(name='frecuencia')
        df_grouped_25_medio = df_grouped_25_medio.sort_values(by='frecuencia', ascending=False)
        df_grouped_25_medio['id_posicion'] = df_grouped_25_medio['id_posicion'].map(id_posicion_dict)
        
        fig_25_medio = px.bar(df_grouped_25_medio,
                            x='nombre_canal',
                            y='frecuencia',
                            color='id_posicion',
                            title = titulo,
                            barmode='stack',
                            color_discrete_map=color_posicion_dict,
                            labels={'frecuencia': 'Frecuencia', 'nombre_canal': 'Canal', 'id_posicion': 'Posición'},
                            text = 'frecuencia',
                            )
        st.plotly_chart(fig_25_medio, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")


    #%% 26.- Se busca conocer las noticias emitidas en un cierto rango de tiempo cuantos son a favor, en contra, neutral, etc
    st.subheader("19.- Notas emitidas en un rango de tiempo segun posicion y coctel")

    col1, col2, col3 = st.columns(3)

    with col1:
        fecha_inicio_g26 = st.date_input(
        "Fecha Inicio g19",
        format="DD.MM.YYYY")

    with col2:
        fecha_fin_g26 = st.date_input(
        "Fecha Fin g19",
        format="DD.MM.YYYY")

    with col3:
        option_nota_g26 = st.selectbox(
            "Nota g19",
            ("Con coctel", "Sin coctel", "Todos")
            )

    fecha_inicio_g26 = pd.to_datetime(fecha_inicio_g26,format='%Y-%m-%d')
    fecha_fin_g26 = pd.to_datetime(fecha_fin_g26,format='%Y-%m-%d')

    temp_g26 = temp_coctel_temas[(temp_coctel_temas['fecha_registro']>=fecha_inicio_g26) &
                                    (temp_coctel_temas['fecha_registro']<=fecha_fin_g26)]

    if option_nota_g26 == 'Con coctel':
        temp_g26 = temp_g26[temp_g26['coctel'] == 1]
        titulo = f"Notas emitidas con coctel entre {fecha_inicio_g26.strftime('%d-%m-%Y')} y {fecha_fin_g26.strftime('%d-%m-%Y')} según posición"
    elif option_nota_g26 == 'Sin coctel':
        temp_g26 = temp_g26[temp_g26['coctel'] == 0]
        titulo = f"Notas emitidas sin coctel entre {fecha_inicio_g26.strftime('%d-%m-%Y')} y {fecha_fin_g26.strftime('%d-%m-%Y')} según posición"
    else:
        titulo = f"Notas emitidas entre {fecha_inicio_g26.strftime('%d-%m-%Y')} y {fecha_fin_g26.strftime('%d-%m-%Y')} según posición"

    if not temp_g26.empty:

        df_grouped_26 = temp_g26.groupby(['id_posicion']).size().reset_index(name='frecuencia')

        df_grouped_26['id_posicion'] = df_grouped_26['id_posicion'].map(id_posicion_dict)

        fig_26 = px.bar(df_grouped_26,
                        x='id_posicion',
                        y='frecuencia',
                        title = titulo,
                        labels={'frecuencia': 'Frecuencia', 'id_posicion': 'Posición'},
                        color='id_posicion',
                        color_discrete_map=color_posicion_dict,
                        text='frecuencia'
                        )

        st.plotly_chart(fig_26, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")

    #%% 27.- grafico de barras sobre actores y posiciones
    st.subheader("20.- Recuento de posiciones emitidas por actor en lugar y fecha específica")

    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio_g27 = st.date_input(
        "Fecha Inicio g20",
        format="DD.MM.YYYY")
    with col2:
        fecha_fin_g27 = st.date_input(
        "Fecha Fin g20",
        format="DD.MM.YYYY")

    col1, col2, col3 = st.columns(3)

    with col1:
        option_fuente_g27 = st.selectbox(
        "Fuente g20",
        ("Radio", "TV", "Redes","Todos"),)

    with col2:
        option_lugar_g27 = st.selectbox(
        "Lugar g20",
        lugares_uniques)

    with col3:
        option_nota_g27 = st.selectbox(
            "Nota g20",
            ("Con coctel", "Sin coctel", "Todos"))


    fecha_inicio_g27 = pd.to_datetime(fecha_inicio_g27,format='%Y-%m-%d')
    fecha_fin_g27 = pd.to_datetime(fecha_fin_g27,format='%Y-%m-%d')

    temp_g27 = temp_coctel_fuente_actores[(temp_coctel_fuente_actores['fecha_registro']>=fecha_inicio_g27) &
                                    (temp_coctel_fuente_actores['fecha_registro']<=fecha_fin_g27) &
                                    (temp_coctel_fuente_actores['lugar']==option_lugar_g27)]

    if option_fuente_g27 == 'Radio':
        temp_g27 = temp_g27[temp_g27['id_fuente']==1]
    elif option_fuente_g27 == 'TV':
        temp_g27 = temp_g27[temp_g27['id_fuente']==2]
    elif option_fuente_g27 == 'Redes':
        temp_g27 = temp_g27[temp_g27['id_fuente']==3]

    if option_nota_g27 == 'Con coctel':
        temp_g27 = temp_g27[temp_g27['coctel'] == 1]
        titulo = f"Recuento de posiciones emitidas por actor en {option_lugar_g27} entre {fecha_inicio_g27.strftime('%d-%m-%Y')} y {fecha_fin_g27.strftime('%d-%m-%Y')}. Notas con coctel"
    elif option_nota_g27 == 'Sin coctel':
        temp_g27 = temp_g27[temp_g27['coctel'] == 0]
        titulo = f"Recuento de posiciones emitidas por actor en {option_lugar_g27} entre {fecha_inicio_g27.strftime('%d-%m-%Y')} y {fecha_fin_g27.strftime('%d-%m-%Y')}. Notas sin coctel"

    else:
        titulo = f"Recuento de posiciones emitidas por actor en {option_lugar_g27} entre {fecha_inicio_g27.strftime('%d-%m-%Y')} y {fecha_fin_g27.strftime('%d-%m-%Y')}"


    if not temp_g27.empty:
        temp_g27["posicion"] = temp_g27["id_posicion"].map(id_posicion_dict)
        temp_g27 = temp_g27[temp_g27["nombre"] != "periodista"]
        
        df_grouped_27 = temp_g27.groupby(['nombre', 'posicion']).size().reset_index(name='frecuencia')

        top_10_actores = df_grouped_27.groupby('nombre')['frecuencia'].sum().nlargest(10).index
        df_top_10_27 = df_grouped_27[df_grouped_27['nombre'].isin(top_10_actores)]

        fig_27 = px.bar(df_top_10_27,
                        x='nombre',
                        y='frecuencia',
                        title = titulo,
                        color='posicion',
                        barmode='stack',
                        color_discrete_map={'a favor': 'blue', 'potencialmente a favor': 'lightblue', 'neutral': 'gray', 'potencialmente en contra': 'orange', 'en contra': 'red'},
                        labels={'frecuencia': 'Frecuencia', 'nombre': 'Actor', 'posicion': 'Posición'},
                        category_orders={'nombre': top_10_actores},
                        text='frecuencia'
                        )

        st.plotly_chart(fig_27, use_container_width=True)

    else:
        st.warning("No hay datos para mostrar")
    
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
    ["Análisis de Cocteles", "Usuarios y Acontecimientos", "Actualizar Datos"]
)

if menu == "Análisis de Cocteles":
    coctel_dashboard()

elif menu == "Usuarios y Acontecimientos":
    usarios_acontecimientos_dashboard()

elif menu == "Actualizar Datos":
    with st.spinner("Actualizando datos, por favor espera..."):
        update_data()
    st.success("Datos actualizados correctamente.")


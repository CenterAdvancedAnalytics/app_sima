import pandas as pd
from sqlalchemy import create_engine, text
# from dotenv import load_dotenv
import os
from pathlib import Path
import streamlit as st

# # Especificar la ruta al archivo .env
# dotenv_path = Path('app/.env')  # Cambia esta ruta por la ubicación de tu archivo .env
# load_dotenv(dotenv_path=dotenv_path)

# # Cargar las variables desde el archivo .env
# DB_USER = os.getenv('DB_USER')
# DB_PASSWORD = os.getenv('DB_PASSWORD')
# DB_HOST = os.getenv('DB_HOST')
# DB_PORT = os.getenv('DB_PORT')
# DB_NAME = os.getenv('DB_NAME')

def update_data():
    DB_USER = st.secrets["DB_USER"]
    DB_PASSWORD = st.secrets["DB_PASSWORD"]
    DB_HOST = st.secrets["DB_HOST"]
    DB_PORT = st.secrets["DB_PORT"]
    DB_NAME = st.secrets["DB_NAME"]

    # URL y motor de conexión
    SQLALCHEMY_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    queries = {
        # Queries de cocteles
        "base": {
            "create": text("""
            CREATE TEMP TABLE temp_base_coctel AS
            SELECT
                a.id AS id,
                a.fecha_registro,
                a.acontecimiento,
                a.coctel,
                l.nombre AS lugar,
                p.color AS color,
                pr.id_fuente AS id_fuente,
                f.nombre AS fuente_nombre,
                pr.id_canal AS id_canal,
                c.nombre AS canal_nombre
            FROM
                acontecimientos a
            JOIN
                lugares l ON a.id_lugar = l.id
            JOIN
                posiciones p ON a.id_posicion = p.id
            LEFT JOIN
                acontecimiento_programa ap ON a.id = ap.id_acontecimiento
            LEFT JOIN
                programas pr ON ap.id_programa = pr.id
            LEFT JOIN
                fuentes f ON pr.id_fuente = f.id
            LEFT JOIN
                canales c ON pr.id_canal = c.id
            WHERE
                a.fecha_registro >= NOW() - INTERVAL '2 years';
            """),
            "read": text("SELECT * FROM temp_base_coctel;")
        },
        "actores": {
            "create": text("""
            CREATE TEMP TABLE temp_coctel_fuente_actores AS
            SELECT
                base.*,
                ac.nombre AS actor_nombre
            FROM
                temp_base_coctel base
            JOIN
                acontecimiento_actor aa ON base.id = aa.id_acontecimiento
            JOIN
                actores ac ON aa.id_actor = ac.id;
            """),
            "read": text("SELECT * FROM temp_coctel_fuente_actores;")
        },
        "fb": {
            "create": text("""
            CREATE TEMP TABLE temp_coctel_fuente_fb AS
            SELECT
                base.*,
                fb.num_reacciones,
                fb.num_comentarios,
                fb.num_compartidos,
                fb.fecha AS fecha_post,
                fbp.nombre AS nombre_facebook_page
            FROM
                temp_base_coctel base
            JOIN
                acontecimiento_facebook_post afb ON base.id = afb.id_acontecimiento
            JOIN
                facebook_posts fb ON afb.id_facebook_post = fb.id
            JOIN
                facebook_pages fbp ON fb.id_facebook_page = fbp.id;
            """),
            "read": text("SELECT * FROM temp_coctel_fuente_fb;")
        },
        "temas": {
            "create": text("""
            CREATE TEMP TABLE temp_coctel_temas AS
            SELECT
                base.*,
                t.descripcion AS tema_descripcion
            FROM
                temp_base_coctel base
            JOIN
                acontecimiento_tema at ON base.id = at.id_acontecimiento
            JOIN
                temas t ON at.id_tema = t.id;
            """),
            "read": text("SELECT * FROM temp_coctel_temas;")
        },
        # Queries adicionales
        "usuarios_por_dia": {
            "create": text("""
            CREATE TEMP TABLE temp_usuarios_por_dia AS
            SELECT
                DATE(a.fecha_registro) AS fecha,
                COUNT(DISTINCT a.id_usuario_registro) AS usuarios_distintos
            FROM
                acontecimientos a
            GROUP BY
                DATE(a.fecha_registro)
            ORDER BY
                fecha;
            """),
            "read": text("SELECT * FROM temp_usuarios_por_dia;")
        },
        "acontecimientos_por_dia": {
            "create": text("""
            CREATE TEMP TABLE temp_acontecimientos_por_dia AS
            SELECT
                DATE(a.fecha_registro) AS fecha,
                COUNT(a.id) AS total_acontecimientos
            FROM
                acontecimientos a
            GROUP BY
                DATE(a.fecha_registro)
            ORDER BY
                fecha;
            """),
            "read": text("SELECT * FROM temp_acontecimientos_por_dia;")
        },
        "usuarios_ultimo_dia": {
            "create": text("""
            CREATE TEMP TABLE temp_usuarios_ultimo_dia AS
            SELECT
                u.id AS id_usuario,
                u.nombre AS nombre_usuario,
                COUNT(a.id) AS total_acontecimientos
            FROM
                usuarios u
            LEFT JOIN
                acontecimientos a ON u.id = a.id_usuario_registro
            WHERE
                DATE(a.fecha_registro) = (
                    SELECT MAX(DATE(fecha_registro)) FROM acontecimientos
                )
            GROUP BY
                u.id, u.nombre
            ORDER BY
                total_acontecimientos DESC;
            """),
            "read": text("SELECT * FROM temp_usuarios_ultimo_dia;")
        },
        "usuarios_7_dias": {
            "create": text("""
            CREATE TEMP TABLE temp_usuarios_7_dias AS
            SELECT
                u.id AS id_usuario,
                u.nombre AS nombre_usuario,
                SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '6 days' THEN 1 ELSE 0 END) AS dia_1,
                SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '5 days' THEN 1 ELSE 0 END) AS dia_2,
                SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '4 days' THEN 1 ELSE 0 END) AS dia_3,
                SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '3 days' THEN 1 ELSE 0 END) AS dia_4,
                SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '2 days' THEN 1 ELSE 0 END) AS dia_5,
                SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE - INTERVAL '1 days' THEN 1 ELSE 0 END) AS dia_6,
                SUM(CASE WHEN DATE(a.fecha_registro) = CURRENT_DATE THEN 1 ELSE 0 END) AS dia_7
            FROM
                usuarios u
            LEFT JOIN
                acontecimientos a ON u.id = a.id_usuario_registro
            WHERE
                DATE(a.fecha_registro) >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY
                u.id, u.nombre
            ORDER BY
                u.nombre;
            """),
            "read": text("SELECT * FROM temp_usuarios_7_dias;")
        }
    }

    try:
        with engine.connect() as connection:
            # Crear y leer tablas
            connection.execute(queries["base"]["create"])
            temp_base_coctel = pd.read_sql(queries["base"]["read"], connection)

            connection.execute(queries["actores"]["create"])
            temp_coctel_fuente_actores = pd.read_sql(queries["actores"]["read"], connection)

            connection.execute(queries["fb"]["create"])
            temp_coctel_fuente_fb = pd.read_sql(queries["fb"]["read"], connection)

            connection.execute(queries["temas"]["create"])
            temp_coctel_temas = pd.read_sql(queries["temas"]["read"], connection)

            connection.execute(queries["usuarios_por_dia"]["create"])
            usuarios_por_dia = pd.read_sql(queries["usuarios_por_dia"]["read"], connection)

            connection.execute(queries["acontecimientos_por_dia"]["create"])
            acontecimientos_por_dia = pd.read_sql(queries["acontecimientos_por_dia"]["read"], connection)

            connection.execute(queries["usuarios_ultimo_dia"]["create"])
            usuarios_ultimo_dia = pd.read_sql(queries["usuarios_ultimo_dia"]["read"], connection)

            connection.execute(queries["usuarios_7_dias"]["create"])
            usuarios_7_dias = pd.read_sql(queries["usuarios_7_dias"]["read"], connection)

        # Guardar resultados en archivos
        temp_base_coctel.to_parquet("app/tables/temp_base_coctel.parquet", index=False)
        temp_coctel_fuente_actores.to_parquet("app/tables/temp_coctel_fuente_actores.parquet", index=False)
        temp_coctel_fuente_fb.to_parquet("app/tables/temp_coctel_fuente_fb.parquet", index=False)
        temp_coctel_temas.to_parquet("app/tables/temp_coctel_temas.parquet", index=False)
        usuarios_por_dia.to_parquet("app/tables/temp_usuarios_por_dia.parquet", index=False)
        acontecimientos_por_dia.to_parquet("app/tables/temp_acontecimientos_por_dia.parquet", index=False)
        usuarios_ultimo_dia.to_parquet("app/tables/temp_usuarios_ultimo_dia.parquet", index=False)
        usuarios_7_dias.to_parquet("app/tables/temp_usuarios_7_dias.parquet", index=False)

        print("Actualización completada y datos guardados en Parquet.")
    except Exception as e:
        print(f"Error durante la ejecución: {e}")

if __name__ == "__main__":
    update_data()

import streamlit as st
import pandas as pd
import numpy as np
import warnings
import os
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import time
import random
import json
import shutil
from pathlib import Path
import zipfile
import hashlib
import logging
import hmac
import pymongo

warnings.filterwarnings("ignore")

#################################################################
# Password protection
# Password
def check_password():
    """Returns `True` if the user had the correct password."""
 
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if hmac.compare_digest(st.session_state["password"], st.secrets["password"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the password.
        else:
            st.session_state["password_correct"] = False
 
    # Return True if the password is# validated.
    if st.session_state.get("password_correct", False):
        return True
 
    # Show input for password.
    st.text_input(
        "Password", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state:
        st.error("😕 Password incorrect")
    return False
 
 
if not check_password():
    st.stop()  # Do not continue if check_password is not True.

#################################################################
# MongoDB Data Loading Functions
@st.cache_resource
def get_data(collection_name):
    """Get data from MongoDB BOTS database for specified collection"""
    mongo_uri = st.secrets["mongouri"]
    client = pymongo.MongoClient(mongo_uri)
    db = client.BOTS
    collection = db[collection_name]
    
    data = list(collection.find())
    df = pd.DataFrame(data)
    
    return df

def load_data(collection_name):
    """Load and process data from MongoDB collection"""
    try:
        df = get_data(collection_name)
        
        if df.empty:
            st.error(f"❌ No se encontraron datos en la colección {collection_name}.")
            st.stop()
        
        df = df.drop(columns=['_id'], errors='ignore')
        
        if 'Fecha' not in df.columns:
            st.error("❌ No se encontró la columna 'Fecha' en los datos.")
            st.stop()
        
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
        df = df.dropna(subset=['Fecha'])
        
        df = df[df['Fecha'] >= '2023-01-01']
        
        numeric_cols = ['Cantidad']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
    except Exception as e:
        st.error(f"Error al cargar los datos de {collection_name}: {str(e)}")
        st.stop()

# Funciones globales con cache para optimizar carga
@st.cache_data
def load_bot_dictionary(bot_name):
    """Carga diccionario de un bot específico con cache"""
    base_path = "/home/jarriola/BOTS"
    
    try:
        if bot_name == "ADOLFO":
            return pd.read_csv(f"{base_path}/ADOLFO/AD.csv", sep=';')
        elif bot_name == "BIRKEN":
            return pd.read_csv(f"{base_path}/BIRKEN/birken.csv", sep=';')
        elif bot_name == "PB":
            return pd.read_csv(f"{base_path}/PB/dictionarypb.csv", sep=';')
        elif bot_name == "SKECHERS":
            return pd.read_csv(f"{base_path}/SKECHERS/dictionarysk.csv", sep=';')
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error cargando diccionario {bot_name}: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def process_new_era_levels():
    """Procesa y combina archivos L1-L7 de NEW ERA con cache"""
    base_path = "/home/jarriola/BOTS/NEW ERA"
    
    try:
        # Cargar archivos L1-L7
        df_l1 = pd.read_csv(f"{base_path}/NE_L1.csv", sep=';')
        df_l2 = pd.read_csv(f"{base_path}/NE_L2.csv", sep=';')
        df_l3 = pd.read_csv(f"{base_path}/NE_L3.csv", sep=';')
        df_l4 = pd.read_csv(f"{base_path}/NE_L4.csv", sep=';')
        df_l5 = pd.read_csv(f"{base_path}/NE_L5.csv", sep=';')
        df_l6 = pd.read_csv(f"{base_path}/NE_L6.csv", sep=';')
        
        # Intentar cargar L7, si no existe usar L6
        try:
            df_l7 = pd.read_csv(f"{base_path}/NE_L7.csv", sep=';')
        except:
            df_l7 = pd.read_csv(f"{base_path}/NE_L6.csv", sep=';')
        
        # Columnas de agrupación
        columnas_agrupacion = ['U_Estilo', 'U_Silueta', 'U_Team', 'U_Descrip_Color', 'U_Segmento',
                              'U_Liga', 'U_Coleccion_NE', 'U_Genero', 'U_Descripcion', 'U_Temporalidad']
        
        # Agrupar cada nivel eliminando duplicados
        dataframes = [df_l1, df_l2, df_l3, df_l4, df_l5, df_l6, df_l7]
        df_grouped_list = []
        
        for df in dataframes:
            # Filtrar solo las columnas que existen en el DataFrame
            columnas_existentes = [col for col in columnas_agrupacion if col in df.columns]
            
            if columnas_existentes:
                df_grouped = df.groupby(columnas_existentes).size().reset_index(name='count')
                df_grouped = df_grouped.drop(columns=['count'])
                df_grouped['U_Estilo'] = df_grouped['U_Estilo'].astype(str)
                df_grouped_list.append(df_grouped)
            else:
                # Si no hay columnas válidas, crear DataFrame vacío
                df_grouped_list.append(pd.DataFrame())
        
        # Combinar en cascada (L1 → L2 → L3 → L4 → L5 → L6 → L7)
        df_final = df_grouped_list[0].copy() if not df_grouped_list[0].empty else pd.DataFrame()
        
        # Columnas comunes para completar
        columnas_comunes = ['U_Silueta', 'U_Team', 'U_Descrip_Color', 'U_Segmento',
                           'U_Liga', 'U_Coleccion_NE', 'U_Genero', 'U_Descripcion', 'U_Temporalidad']
        
        # Proceso de combinación en cascada
        for df_temp in df_grouped_list[1:]:
            if df_temp.empty:
                continue
                
            # Merge con el DataFrame actual
            df_merged = df_final.merge(
                df_temp,
                on='U_Estilo',
                how='left',
                suffixes=('', '_temp')
            )
            
            # Completar valores faltantes en las columnas comunes
            for col in columnas_comunes:
                if col in df_final.columns and f'{col}_temp' in df_merged.columns:
                    df_merged[col] = df_merged[col].fillna(df_merged[f'{col}_temp'])
                    df_merged.drop(f'{col}_temp', axis=1, inplace=True)
            
            # Añadir registros únicos del DataFrame temporal que no están en df_final
            df_final = pd.concat([
                df_merged,
                df_temp[~df_temp['U_Estilo'].isin(df_final['U_Estilo'])]
            ], ignore_index=True)
        
        # Eliminar posibles duplicados
        df_final = df_final.drop_duplicates(subset='U_Estilo', keep='first')
        
        return df_final
        
    except Exception as e:
        st.error(f"❌ Error procesando archivos NEW ERA L1-L7: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def process_ch_levels():
    """Procesa y combina archivos L1-L6 de CH con cache"""
    base_path = "/home/jarriola/BOTS/CH"
    
    try:
        # Cargar archivos L1-L6
        df_l1 = pd.read_csv(f"{base_path}/CH_L1.csv", sep=';')
        df_l2 = pd.read_csv(f"{base_path}/CH_L2.csv", sep=';')
        df_l3 = pd.read_csv(f"{base_path}/CH_L3.csv", sep=';')
        df_l4 = pd.read_csv(f"{base_path}/CH_L4.csv", sep=';')
        df_l5 = pd.read_csv(f"{base_path}/CH_L5.csv", sep=';')
        df_l6 = pd.read_csv(f"{base_path}/CH_L6.csv", sep=';')
        
        # Columnas de agrupación para CH
        columnas_agrupacion = ['U_Estilo', 'U_Descripcion', 'U_Segmentacion_SK', 'U_Zone', 'U_Descrip_Color']
        
        # Agrupar cada nivel eliminando duplicados
        dataframes = [df_l1, df_l2, df_l3, df_l4, df_l5, df_l6]
        df_grouped_list = []
        
        for df in dataframes:
            # Filtrar solo las columnas que existen en el DataFrame
            columnas_existentes = [col for col in columnas_agrupacion if col in df.columns]
            
            if columnas_existentes:
                df_grouped = df.groupby(columnas_existentes).size().reset_index(name='count')
                df_grouped = df_grouped.drop(columns=['count'])
                df_grouped['U_Estilo'] = df_grouped['U_Estilo'].astype(str)
                df_grouped_list.append(df_grouped)
            else:
                # Si no hay columnas válidas, crear DataFrame vacío
                df_grouped_list.append(pd.DataFrame())
        
        # Combinar en cascada según prioridad del notebook (L2 → L3 → L4 → L5 → L6 → L1)
        dataframes_a_combinar = [df_grouped_list[2], df_grouped_list[3], df_grouped_list[4], df_grouped_list[5], df_grouped_list[0]]  # L3,L4,L5,L6,L1
        df_final = df_grouped_list[1].copy() if not df_grouped_list[1].empty else pd.DataFrame()  # Empezar con L2
        
        # Columnas comunes para completar
        columnas_comunes = ['U_Descripcion', 'U_Segmentacion_SK', 'U_Zone', 'U_Descrip_Color']
        
        # Proceso de combinación en cascada
        for df_temp in dataframes_a_combinar:
            if df_temp.empty:
                continue
                
            # Merge con el DataFrame actual
            df_merged = df_final.merge(
                df_temp,
                on='U_Estilo',
                how='left',
                suffixes=('', '_temp')
            )
            
            # Completar valores faltantes en las columnas comunes
            for col in columnas_comunes:
                if col in df_final.columns and f'{col}_temp' in df_merged.columns:
                    df_merged[col] = df_merged[col].fillna(df_merged[f'{col}_temp'])
                    df_merged.drop(f'{col}_temp', axis=1, inplace=True)
            
            # Añadir registros únicos del DataFrame temporal que no están en df_final
            df_final = pd.concat([
                df_merged,
                df_temp[~df_temp['U_Estilo'].isin(df_final['U_Estilo'])]
            ], ignore_index=True)
        
        # Eliminar posibles duplicados
        df_final = df_final.drop_duplicates(subset='U_Estilo', keep='first')
        
        return df_final
        
    except Exception as e:
        st.error(f"❌ Error procesando archivos CH L1-L6: {str(e)}")
        return pd.DataFrame()

class DataCleaner:
    def __init__(self):
        # Inicializar diccionarios como None - se cargarán bajo demanda
        self.adolfo_dict = None
        self.birken_dict = None
        self.pb_dict = None
        self.skechers_dict = None
        self.new_era_dict = None
        self.ch_dict = None
        
        # Diccionario de licencias para NEW ERA
        self.team_licenses = {
            'LOS ANGELES DODGERS': 'MLB',
            'NEW YORK YANKEES': 'MLB',
            'PITTSBURGH PIRATES': 'MLB',
            'SAN FRANCISCO GIANTS': 'MLB',
            'SEATTLE MARINERS': 'MLB',
            'TAMPA BAY RAYS': 'MLB',
            'NEW ERA BRANDED': 'NEW ERA BRANDED',
            'NO APLICA': 'NO APLICA',
            'NEW ENGLAND PATRIOTS': 'NFL',
            'HOUSTON TEXANS': 'NFL',
            'BALTIMORE RAVENS': 'NFL',
            'TORONTO BLUE JAYS': 'MLB',
            'HOUSTON ASTROS': 'MLB',
            'GREEN BAY PACKERS': 'NFL',
            'BOSTON RED SOX': 'MLB',
            'BALTIMORE ORIOLES': 'MLB',
            'ST. LOUIS CARDINALS': 'MLB',
            'SEATTLE SEAHAWKS': 'NFL',
            'DALLAS COWBOYS': 'NFL',
            'PITTSBURGH STEELERS': 'NFL',
            'MIAMI DOLPHINS': 'NFL',
            'STARWARS': 'ENTERTAINMENT',
            'DALLAS MAVERICKS': 'NBA',
            'LOS ANGELES LAKERS': 'NBA',
            'NEW ORLEANS SAINTS': 'NFL',
            'JACKSONVILLE JAGUARS': 'NFL',
            'CLEVELAND BROWNS': 'NFL',
            'NEW YORK KNICKS': 'NBA',
            'SAN ANTONIO SPURS': 'NBA',
            'WASHINGTON NATIONALS': 'MLB',
            'OAKLAND ATHLETICS': 'MLB',
            'DETROIT TIGERS': 'MLB',
            'ANAHEIM ANGELS': 'MLB',
            'NASCAR': 'MOTORSPORT',
            'NEW YORK METS': 'MLB',
            'PHILADELPHIA PHILLIES': 'MLB',
            'CHICAGO WHITE SOX': 'MLB',
            'SAN DIEGO PADRES': 'MLB',
            'CLEVELAND INDIANS': 'MLB',
            'DENVER BRONCOS': 'NFL',
            'BUFFALO BILLS': 'NFL',
            'ATLANTA FALCONS': 'NFL',
            'CHICAGO BEARS': 'NFL',
            'BROOKLYN NETS': 'NBA',
            'CHICAGO BULLS': 'NBA',
            'SAN FRANCISCO 49ERS': 'NFL',
            'INDIANAPOLIS COLTS': 'NFL',
            'ARIZONA CARDINALS': 'NFL',
            'OAKLAND RAIDERS': 'NFL',
            'LOS ANGELES RAMS': 'NFL',
            'TAMPA BAY BUCCANEERS': 'NFL',
            'GOLDEN STATE WARRIORS': 'NBA',
            'BOSTON CELTICS': 'NBA',
            'CHICAGO CUBS': 'MLB',
            'TWEETY PIE': 'ENTERTAINMENT',
            'TOY STORY': 'ENTERTAINMENT',
            'ATLANTA BRAVES': 'MLB',
            'SPIDERMAN': 'ENTERTAINMENT',
            'DEADPOOL': 'ENTERTAINMENT',
            'PHILADELPHIA EAGLES': 'NFL',
            'DETROIT LIONS': 'NFL',
            'MILWAUKEE BUCKS': 'NBA',
            'MIAMI HEAT': 'NBA',
            'FLORIDA MARLINS': 'MLB',
            'MCLAREN RACING': 'MOTORSPORT',
            'MICKEY MOUSE': 'ENTERTAINMENT',
            'WASHINGTON REDSKINS': 'NFL',
            'LAS VEGAS RAIDERS': 'NFL',
            'NEW YORK GIANTS': 'NFL',
            'CINCINNATI REDS': 'MLB',
            'HULK': 'ENTERTAINMENT',
            'KANSAS CITY CHIEFS': 'NFL',
            'NEW ORLEANS PELICANS': 'NBA',
            'DENVER NUGGETS': 'NBA',
            'MINNESOTA VIKINGS': 'NFL',
            'MIAMI MARLINS': 'MLB',
            'TENNESSEE TITANS': 'NFL',
            'UNKNOWN': 'MLB',
            'CAPTAIN AMERICA': 'ENTERTAINMENT',
            'PHOENIX SUNS': 'NBA',
            'MLB GENERIC LOGO': 'MLB',
            'BROOKLYN DODGERS': 'MLB',
            'SYLVESTER': 'ENTERTAINMENT',
            'ORLANDO MAGIC': 'NBA',
            'ARIZONA DIAMONDBACKS': 'MLB',
            'TEXAS RANGERS': 'MLB',
            'LOONEY TUNES': 'ENTERTAINMENT',
            'INDUSTRIA INC': 'ENTERTAINMENT',
            'FORMULA E': 'MOTORSPORT',
            'CLUB DEPORTIVO OLIMPIA': 'HONDURAS SOCCER',
            'CAROLINA PANTHERS': 'NFL',
            'MINNESOTA TWINS': 'MLB',
            'JUSTICE LEAGUE': 'ENTERTAINMENT',
            'COLORADO ROCKIES': 'MLB',
            'TORONTO RAPTORS': 'NBA',
            'ANIMANIACS': 'ENTERTAINMENT',
            'MONSTERS INC': 'ENTERTAINMENT',
            'HOUSTON OILERS': 'NFL',
            'SUPERMAN': 'ENTERTAINMENT',
            'BATMAN': 'ENTERTAINMENT',
            'KANSAS CITY ROYALS': 'MLB',
            'LOS ANGELES CLIPPERS': 'NBA',
            'MINNIE MOUSE': 'ENTERTAINMENT',
            'CLUB SOCIAL DEPORTIVO MUNICIPAL': 'GUATEMALA SOCCER LEAGUE',
            'EMF': 'ENTERTAINMENT',
            'UTAH JAZZ': 'NBA',
            'NEW YORK JETS': 'NFL',
            'HOUSTON ROCKETS': 'NBA',
            'LED ZEPPELIN': 'ENTERTAINMENT',
            'GUATEMALA': 'MARCA PAIS',
            'LEHIGH VALLEY IRON PIGS': 'MILB',
            'FRESNO GRIZZLIES': 'MILB',
            'NFL GENERIC LOGO': 'NFL',
            'PLUTO': 'ENTERTAINMENT',
            'MONTREAL EXPOS': 'MLB',
            'MILWAUKEE BREWERS': 'MLB',
            'GENERIC DISNEY': 'ENTERTAINMENT',
            'BUGS BUNNY': 'ENTERTAINMENT',
            'MEMPHIS CHICKS': 'MILB',
            'PIGLET': 'ENTERTAINMENT',
            'LOS ANGELES CHARGERS': 'NFL',
            'GOOFY': 'ENTERTAINMENT',
            'TOM & JERRY': 'ENTERTAINMENT',
            'HARTFORD YARD GOATS': 'MILB',
            'PHILADELPHIA 76ERS': 'NBA',
            'CINCINNATI BENGALS': 'NFL',
            'ATLANTA HAWKS': 'NBA',
            'FEAR OF GOD': 'FEAR OF GOD',
            'NBA LOGO': 'NBA',
            'LA CASA DE PAPEL': 'ENTERTAINMENT',
            'NFL OFFICIAL LOGO': 'NFL',
            'TELLAECHE': 'TELLAECHE',
            'COMUNICACIONES FC': 'GUATEMALA SOCCER LEAGUE',
            'LOS ANGELES ANGELS': 'MLB',
            'PANAMA': 'MARCA PAIS',
            'TEAM GLISTEN': 'ENTERTAINMENT',
            'TIGGER': 'ENTERTAINMENT',
            'PUERTO RICO': 'FEDERACION DE BASEBALL PUERTO RICO',
            'EL SALVADOR': 'MARCA PAIS',
            'GOLDEN STATE WARRIOR': 'NBA',
            'CHARLOTTE HORNETS': 'NBA',
            'NFL GENERIC SUPERBOW': 'NFL',
            'BIZARRAP': 'ENTERTAINMENT',
            'NFL ALL OVER': 'NFL',
            'DOMINICAN REPUBLIC': 'FEDERACION DOMINICANA DE BASEBALL',
            'KOUMORI': 'NEW ERA BRANDED',
            'WINNIE THE POOH': 'ENTERTAINMENT',
            'OKLAHOMA CITY THUNDER': 'NBA',
            'MINNESOTA TIMBERWOLVES': 'NBA',
            'MLB PROPERTIES ALL OVER': 'MLB',
            'NBA ALL OVER': 'NBA',
            'CLEVELAND CAVALIERS': 'NBA',
            'WASHINGTON': 'NFL',
            'DISNEY': 'ENTERTAINMENT',
            'TAZ': 'ENTERTAINMENT',
            'DAFFY DUCK': 'ENTERTAINMENT',
            'MLB ALL OVER': 'MLB',
            'PORTLAND TRAIL BLAZE': 'NBA',
            'SCOOBY DOO': 'ENTERTAINMENT',
            'BROOKLYN CYCLONES': 'MILB',
            'SACRAMENTO KINGS': 'NBA',
            'DOMINICAN REPUBLIC C': 'BASEBALL FEDERATION',
            'TEEN TITAN STAR FIRE': 'ENTERTAINMENT',
            'COLUMBUS CREW': 'MLS',
            'SEATTLE SOUNDERS': 'MLS',
            'MCLAREN F1 RACING': 'MOTORSPORT',
            'MLB LOGO': 'MLB',
            'SPACE JAM': 'NBA',
            'MANCHESTER UNITED FC': 'EUROPEAN SOCCER',
            'CHELSEA FC': 'EUROPEAN SOCCER',
            'DUCATI': 'MOTORSPORT',
            'MERCEDES BENZ': 'MOTORSPORT',
            'NBA ALL STAR GAME': 'NBA',
            'RENAULT F1 RACING': 'MOTORSPORT',
            'NEW YORK RED BULLS': 'MLS',
            'INTER MIAMI': 'MLS',
            'NEW YORK CITY FC': 'MLS',
            'NFL LOGO': 'NFL',
            'RENAULT F1': 'MOTORSPORT',
            'TEEN TITAN CYBORG': 'ENTERTAINMENT',
            'L.A. GALAXY': 'MLS',
            'TEEN TITAN RAVEN': 'ENTERTAINMENT',
            'LAKELAND TIGERS': 'MILB',
            'RED BULL F1': 'MOTORSPORT',
            'CALIFORNIA ANGELS': 'MLB',
            'READING FIGHTIN PHILS': 'MILB',
            'CLEVELAND 2022': 'MLB',
            'LOS ANGELES FC': 'MLS',
            'WASHINGTON COMMANDER': 'NFL',
            'DETROIT PISTONS': 'NBA',
            'CLEVELAND GUARDIANS': 'MLB',
            'CHELSEA FC LION CREST': 'EUROPEAN SOCCER',
            'TEEN TITAN ROBIN': 'ENTERTAINMENT',
            'WBC MEXICO': 'WBC',
            'WBC VENEZUELA': 'WBC',
            'WBC PUERTO RICO': 'WBC',
            'MLB ALL STAR GAME LOGO': 'MLB',
            'MLB GENERIC WORLD SERIES': 'MLB',
            'TUCSON SIDEWINDERS': 'MILB',
            'WB 100TH LOONEY TUNES MASHUPS': 'ENTERTAINMENT',
            'MEMPHIS GRIZZLIES': 'NBA',
            'OKLAHOMA CITY THUNDE': 'NBA',
            'PORTLAND TRAIL BLAZERS': 'NBA',
            'HAAS FORMULA 1': 'MOTORSPORT',
            'RED BULL F1 RACING': 'MOTORSPORT',
            'WB HARRY POTTER DEAT': 'ENTERTAINMENT',
            'WB HARRY POTTER DEATHLY HOLLOW PT 2': 'ENTERTAINMENT',
            'WBC PANAMA': 'WBC',
            'WILE E COYOTE': 'ENTERTAINMENT',
            'GRAPEFRUIT LEAGUE LO': 'MLB',
            'LAS VEGAS AVIATORS': 'MILB',
            'MCLAREN AUTOMOTIVE': 'MOTORSPORT',
            'BOSTON BRAVES': 'MLB',
            'PHILADELPHIA PHILLIE': 'MLB',
            'CLUB DEPORTIVO OLIMP': 'HONDURAS SOCCER',
            'CANGREJEROS DE SANTU': 'PUERTO RICO BASEBALL',
            'BEAVIS AND BUTT-HEAD': 'ENTERTAINMENT',
            'HONDURAS': 'MARCA PAIS',
            'VISA CASH APP RACING': 'MOTORSPORT',
            'WILLY WONKA': 'ENTERTAINMENT',
            'ALPINE RACING': 'MOTORSPORT',
            'TRUE': 'NEW ERA BRANDED',
            'BRAND NEW ERA': 'NEW ERA BRANDED',
            'MANDALORIAN': 'ENTERTAINMENT',
            'IRONMAN': 'ENTERTAINMENT',
            'INCREDIBLE HULK': 'ENTERTAINMENT',
            'NFL OILERS': 'NFL',
            'WBC DOMINICAN REPUBLIC': 'WBC',
            'MLB MULTI TEAM': 'MLB',
            'A NIGHTMARE ON ELM S': 'ENTERTAINMENT',
            'INTERNATIONAL SPEEDW': 'MOTORSPORT',
            'SPONGEBOB': 'ENTERTAINMENT',
            'HEY ARNOLD': 'ENTERTAINMENT',
            'FRIDAY THE 13TH': 'ENTERTAINMENT',
            'DONALD DUCK': 'ENTERTAINMENT',
            'ENTREGAS DESCONOCIDAS': 'NO APLICA',
            'PATRICK STAR': 'ENTERTAINMENT',
            'ELMER FUDD': 'ENTERTAINMENT',
            'DOWN EAST WOOD DUCKS': 'ENTERTAINMENT',
            'CORPUS CRISTI HOOKS': 'MILB',
            'ENTREGAS A PERSONAL': 'NO APLICA'
        }
    
    @st.cache_data
    def load_adolfo_dict(_self):
        """Carga diccionario ADOLFO con cache"""
        try:
            return pd.read_csv("/home/jarriola/BOTS/ADOLFO/AD.csv", sep=';')
        except Exception as e:
            st.error(f"❌ Error cargando diccionario ADOLFO: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data
    def load_birken_dict(_self):
        """Carga diccionario BIRKEN con cache"""
        try:
            return pd.read_csv("/home/jarriola/BOTS/BIRKEN/birken.csv", sep=';')
        except Exception as e:
            st.error(f"❌ Error cargando diccionario BIRKEN: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data
    def load_pb_dict(_self):
        """Carga diccionario PB con cache"""
        try:
            return pd.read_csv("/home/jarriola/BOTS/PB/dictionarypb.csv", sep=';')
        except Exception as e:
            st.error(f"❌ Error cargando diccionario PB: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data
    def load_skechers_dict(_self):
        """Carga diccionario SKECHERS con cache"""
        try:
            return pd.read_csv("/home/jarriola/BOTS/SKECHERS/dictionarysk.csv", sep=';')
        except Exception as e:
            st.error(f"❌ Error cargando diccionario SKECHERS: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data
    def load_new_era_dict(_self):
        """Procesa y combina archivos L1-L7 de NEW ERA con cache"""
        base_path = "/home/jarriola/BOTS/NEW ERA"
        
        try:
            # Cargar archivos L1-L7
            df_l1 = pd.read_csv(f"{base_path}/NE_L1.csv", sep=';')
            df_l2 = pd.read_csv(f"{base_path}/NE_L2.csv", sep=';')
            df_l3 = pd.read_csv(f"{base_path}/NE_L3.csv", sep=';')
            df_l4 = pd.read_csv(f"{base_path}/NE_L4.csv", sep=';')
            df_l5 = pd.read_csv(f"{base_path}/NE_L5.csv", sep=';')
            df_l6 = pd.read_csv(f"{base_path}/NE_L6.csv", sep=';')
            
            # Intentar cargar L7, si no existe usar L6
            try:
                df_l7 = pd.read_csv(f"{base_path}/NE_L7.csv", sep=';')
            except:
                df_l7 = pd.read_csv(f"{base_path}/NE_L6.csv", sep=';')
            
            # Procesar y combinar (lógica simplificada para velocidad)
            columnas_agrupacion = ['U_Estilo', 'U_Silueta', 'U_Team', 'U_Descrip_Color', 'U_Segmento',
                                  'U_Liga', 'U_Coleccion_NE', 'U_Genero', 'U_Descripcion', 'U_Temporalidad']
            
            # Solo tomar L1 para velocidad - se puede expandir si se necesita
            df_final = df_l1.groupby(['U_Estilo']).first().reset_index()
            df_final['U_Estilo'] = df_final['U_Estilo'].astype(str)
            
            return df_final
            
        except Exception as e:
            st.error(f"❌ Error procesando NEW ERA: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data
    def load_ch_dict(_self):
        """Procesa y combina archivos L1-L6 de CH con cache"""
        base_path = "/home/jarriola/BOTS/CH"
        
        try:
            # Cargar archivos L1-L6
            df_l1 = pd.read_csv(f"{base_path}/CH_L1.csv", sep=';')
            df_l2 = pd.read_csv(f"{base_path}/CH_L2.csv", sep=';')
            
            # Solo usar L1 y L2 para velocidad - se puede expandir si se necesita
            df_final = df_l2.groupby(['U_Estilo']).first().reset_index()
            df_final['U_Estilo'] = df_final['U_Estilo'].astype(str)
            
            return df_final
            
        except Exception as e:
            st.error(f"❌ Error procesando CH: {str(e)}")
            return pd.DataFrame()
    
    def get_adolfo_dict(self):
        """Obtiene diccionario ADOLFO (lazy loading)"""
        if self.adolfo_dict is None:
            self.adolfo_dict = load_bot_dictionary("ADOLFO")
        return self.adolfo_dict
    
    def get_birken_dict(self):
        """Obtiene diccionario BIRKEN (lazy loading)"""
        if self.birken_dict is None:
            self.birken_dict = load_bot_dictionary("BIRKEN")
        return self.birken_dict
    
    def get_pb_dict(self):
        """Obtiene diccionario PB (lazy loading)"""
        if self.pb_dict is None:
            self.pb_dict = load_bot_dictionary("PB")
        return self.pb_dict
    
    def get_skechers_dict(self):
        """Obtiene diccionario SKECHERS (lazy loading)"""
        if self.skechers_dict is None:
            self.skechers_dict = load_bot_dictionary("SKECHERS")
        return self.skechers_dict
    
    def get_new_era_dict(self):
        """Obtiene diccionario NEW ERA (lazy loading)"""
        if self.new_era_dict is None:
            self.new_era_dict = process_new_era_levels()
        return self.new_era_dict
    
    def get_ch_dict(self):
        """Obtiene diccionario CH (lazy loading)"""
        if self.ch_dict is None:
            self.ch_dict = process_ch_levels()
        return self.ch_dict
    
    def load_embedded_dictionaries(self):
        """Carga los diccionarios embebidos desde los archivos"""
        base_path = "/home/jarriola/BOTS"
        
        try:
            # ADOLFO
            self.adolfo_dict = pd.read_csv(f"{base_path}/ADOLFO/AD.csv", sep=';')
            
            # BIRKEN
            self.birken_dict = pd.read_csv(f"{base_path}/BIRKEN/birken.csv", sep=';')
            
            # PB
            self.pb_dict = pd.read_csv(f"{base_path}/PB/dictionarypb.csv", sep=';')
            
            # SKECHERS
            self.skechers_dict = pd.read_csv(f"{base_path}/SKECHERS/dictionarysk.csv", sep=';')
            
            st.success("✅ Diccionarios cargados exitosamente")
            
        except Exception as e:
            st.error(f"❌ Error cargando diccionarios: {str(e)}")
            # Crear diccionarios vacíos como fallback
            self.adolfo_dict = pd.DataFrame()
            self.birken_dict = pd.DataFrame()
            self.pb_dict = pd.DataFrame()
            self.skechers_dict = pd.DataFrame()
            self.ch_dict = pd.DataFrame()
    
    def process_new_era_levels(self):
        """Procesa y combina archivos L1-L7 de NEW ERA según lógica del notebook"""
        base_path = "/home/jarriola/BOTS/NEW ERA"
        
        try:
            # Cargar archivos L1-L7
            df_l1 = pd.read_csv(f"{base_path}/NE_L1.csv", sep=';')
            df_l2 = pd.read_csv(f"{base_path}/NE_L2.csv", sep=';')
            df_l3 = pd.read_csv(f"{base_path}/NE_L3.csv", sep=';')
            df_l4 = pd.read_csv(f"{base_path}/NE_L4.csv", sep=';')
            df_l5 = pd.read_csv(f"{base_path}/NE_L5.csv", sep=';')
            df_l6 = pd.read_csv(f"{base_path}/NE_L6.csv", sep=';')
            
            # Intentar cargar L7, si no existe usar L6
            try:
                df_l7 = pd.read_csv(f"{base_path}/NE_L7.csv", sep=';')
            except:
                df_l7 = pd.read_csv(f"{base_path}/NE_L6.csv", sep=';')  # Fallback como en notebook
            
            # Columnas de agrupación
            columnas_agrupacion = ['U_Estilo', 'U_Silueta', 'U_Team', 'U_Descrip_Color', 'U_Segmento',
                                  'U_Liga', 'U_Coleccion_NE', 'U_Genero', 'U_Descripcion', 'U_Temporalidad']
            
            # Agrupar cada nivel eliminando duplicados
            dataframes = [df_l1, df_l2, df_l3, df_l4, df_l5, df_l6, df_l7]
            df_grouped_list = []
            
            for df in dataframes:
                # Filtrar solo las columnas que existen en el DataFrame
                columnas_existentes = [col for col in columnas_agrupacion if col in df.columns]
                
                if columnas_existentes:
                    df_grouped = df.groupby(columnas_existentes).size().reset_index(name='count')
                    df_grouped = df_grouped.drop(columns=['count'])
                    df_grouped['U_Estilo'] = df_grouped['U_Estilo'].astype(str)
                    df_grouped_list.append(df_grouped)
                else:
                    # Si no hay columnas válidas, crear DataFrame vacío
                    df_grouped_list.append(pd.DataFrame())
            
            # Combinar en cascada (L1 → L2 → L3 → L4 → L5 → L6 → L7)
            df_final = df_grouped_list[0].copy() if not df_grouped_list[0].empty else pd.DataFrame()
            
            # Columnas comunes para completar
            columnas_comunes = ['U_Silueta', 'U_Team', 'U_Descrip_Color', 'U_Segmento',
                               'U_Liga', 'U_Coleccion_NE', 'U_Genero', 'U_Descripcion', 'U_Temporalidad']
            
            # Proceso de combinación en cascada
            for df_temp in df_grouped_list[1:]:
                if df_temp.empty:
                    continue
                
                # Merge con el DataFrame actual (left join)
                df_merged = df_final.merge(
                    df_temp,
                    on='U_Estilo',
                    how='left',
                    suffixes=('', '_temp')
                )
                
                # Completar valores faltantes en las columnas comunes
                for col in columnas_comunes:
                    if col in df_final.columns and f'{col}_temp' in df_merged.columns:
                        df_merged[col] = df_merged[col].fillna(df_merged[f'{col}_temp'])
                        df_merged.drop(f'{col}_temp', axis=1, inplace=True)
                
                # Añadir registros únicos del DataFrame temporal
                df_final = pd.concat([
                    df_merged,
                    df_temp[~df_temp['U_Estilo'].isin(df_final['U_Estilo'])]
                ], ignore_index=True)
            
            # Eliminar duplicados
            if not df_final.empty:
                df_final = df_final.drop_duplicates(subset='U_Estilo', keep='first')
            
            self.new_era_dict = df_final
            
            st.success(f"✅ NEW ERA procesado: {len(df_final)} registros combinados desde L1-L7")
            
        except Exception as e:
            st.error(f"❌ Error procesando archivos NEW ERA L1-L7: {str(e)}")
            # Crear diccionario vacío como fallback
            self.new_era_dict = pd.DataFrame()
    
    def process_ch_levels(self):
        """Procesa y combina archivos L1-L6 de CH según lógica del notebook"""
        base_path = "/home/jarriola/BOTS/CH"
        
        try:
            # Cargar archivos L1-L6
            df_l1 = pd.read_csv(f"{base_path}/CH_L1.csv", sep=';')
            df_l2 = pd.read_csv(f"{base_path}/CH_L2.csv", sep=';')
            df_l3 = pd.read_csv(f"{base_path}/CH_L3.csv", sep=';')
            df_l4 = pd.read_csv(f"{base_path}/CH_L4.csv", sep=';')
            df_l5 = pd.read_csv(f"{base_path}/CH_L5.csv", sep=';')
            df_l6 = pd.read_csv(f"{base_path}/CH_L6.csv", sep=';')
            
            # Columnas de agrupación para CH
            columnas_agrupacion = ['U_Estilo', 'U_Descripcion', 'U_Segmentacion_SK', 'U_Zone', 'U_Descrip_Color']
            
            # Agrupar cada nivel eliminando duplicados
            dataframes = [df_l1, df_l2, df_l3, df_l4, df_l5, df_l6]
            df_grouped_list = []
            
            for df in dataframes:
                # Filtrar solo las columnas que existen en el DataFrame
                columnas_existentes = [col for col in columnas_agrupacion if col in df.columns]
                
                if columnas_existentes:
                    df_grouped = df.groupby(columnas_existentes).size().reset_index(name='count')
                    df_grouped = df_grouped.drop(columns=['count'])
                    df_grouped['U_Estilo'] = df_grouped['U_Estilo'].astype(str)
                    df_grouped_list.append(df_grouped)
                else:
                    # Si no hay columnas válidas, crear DataFrame vacío
                    df_grouped_list.append(pd.DataFrame())
            
            # Combinar en cascada según prioridad del notebook (L2 → L3 → L4 → L5 → L6 → L1)
            dataframes_a_combinar = [df_grouped_list[2], df_grouped_list[3], df_grouped_list[4], df_grouped_list[5], df_grouped_list[0]]  # L3,L4,L5,L6,L1
            df_final = df_grouped_list[1].copy() if not df_grouped_list[1].empty else pd.DataFrame()  # Empezar con L2
            
            # Columnas comunes para completar
            columnas_comunes = ['U_Descripcion', 'U_Segmentacion_SK', 'U_Zone', 'U_Descrip_Color']
            
            # Proceso de combinación en cascada
            for df_temp in dataframes_a_combinar:
                if df_temp.empty:
                    continue
                    
                # Merge con el DataFrame actual
                df_merged = df_final.merge(
                    df_temp,
                    on='U_Estilo',
                    how='left',
                    suffixes=('', '_temp')
                )
                
                # Completar valores faltantes en las columnas comunes
                for col in columnas_comunes:
                    if col in df_final.columns and f'{col}_temp' in df_merged.columns:
                        df_merged[col] = df_merged[col].fillna(df_merged[f'{col}_temp'])
                        df_merged.drop(f'{col}_temp', axis=1, inplace=True)
                
                # Añadir registros únicos del DataFrame temporal que no están en df_final
                df_final = pd.concat([
                    df_merged,
                    df_temp[~df_temp['U_Estilo'].isin(df_final['U_Estilo'])]
                ], ignore_index=True)
            
            # Eliminar posibles duplicados
            df_final = df_final.drop_duplicates(subset='U_Estilo', keep='first')
            
            # Guardar diccionario CH procesado
            self.ch_dict = df_final
            
            st.success(f"✅ CH procesado: {len(df_final)} registros combinados desde L1-L6")
            
        except Exception as e:
            st.error(f"❌ Error procesando archivos CH L1-L6: {str(e)}")
            # Crear diccionario vacío como fallback
            self.ch_dict = pd.DataFrame()
    
    def clean_adolfo_data(self, df_null: pd.DataFrame) -> pd.DataFrame:
        """Limpia datos de ADOLFO usando diccionario embebido - Lógica exacta del notebook"""
        adolfo_dict = self.get_adolfo_dict()
        if adolfo_dict.empty:
            st.error("❌ Diccionario de ADOLFO no disponible")
            return df_null
        
        # Extraer u_estilo de ItemName (lo que está antes del primer '/')
        df_null['u_estilo'] = df_null['ItemName'].str.split('/').str[0]
        
        # Condición 1: que tenga al menos un "/"
        tiene_slash = df_null['ItemName'].str.contains('/')
        
        # Condición 2: que u_estilo tenga al menos 2 caracteres y cumpla formato letra+número
        formato_correcto = (
            df_null['u_estilo'].str.len() >= 2 &
            df_null['u_estilo'].str[0].str.isalpha() &
            df_null['u_estilo'].str[1].str.isnumeric()
        )
        
        # Condición final: válido si cumple ambas
        cond_valido = tiene_slash & formato_correcto
        
        # Separar válidos e inválidos
        df_valido = df_null[cond_valido].copy()
        df_invalido = df_null[~cond_valido].copy()
        
        # Limpiar u_estilo en los inválidos
        df_invalido['u_estilo'] = np.nan
        
        # Para VÁLIDOS: extraer u_descrip_color
        df_valido['u_descrip_color'] = df_valido['ItemName'].str.split('/').str[3]
        
        # Merge para válidos
        ad_temp = adolfo_dict[['ItemCode', 'u_categoria', 'u_genero', 'u_familia']].rename(
            columns={
                'u_categoria': 'u_categoria_ad',
                'u_genero': 'u_genero_ad',
                'u_familia': 'u_familia_ad'
            }
        )
        df_valido = df_valido.merge(ad_temp, on='ItemCode', how='left')
        
        # Llenar columnas en df_valido solo si están vacías
        for col in ['u_categoria', 'u_genero', 'u_familia']:
            # Crear columna si no existe
            if col not in df_valido.columns:
                df_valido[col] = np.nan
            
            if f'{col}_ad' in df_valido.columns:
                df_valido[col] = df_valido[f'{col}_ad'].combine_first(df_valido[col])
                df_valido.drop(columns=[f'{col}_ad'], inplace=True)
        
        # Para INVÁLIDOS: merge con todas las columnas
        ad_temp = adolfo_dict[['ItemCode', 'u_categoria', 'u_genero', 'u_familia', 'u_estilo', 'u_descrip_color']].rename(
            columns={
                'u_categoria': 'u_categoria_ad',
                'u_genero': 'u_genero_ad',
                'u_familia': 'u_familia_ad',
                'u_estilo': 'u_estilo_ad',
                'u_descrip_color': 'u_descrip_color_ad'
            }
        )
        df_invalido = df_invalido.merge(ad_temp, on='ItemCode', how='left')
        
        # Llenar columnas en df_invalido solo si están vacías
        for col in ['u_categoria', 'u_genero', 'u_familia', 'u_estilo', 'u_descrip_color']:
            # Crear columna si no existe
            if col not in df_invalido.columns:
                df_invalido[col] = np.nan
            
            if f'{col}_ad' in df_invalido.columns:
                df_invalido[col] = df_invalido[f'{col}_ad'].combine_first(df_invalido[col])
                df_invalido.drop(columns=[f'{col}_ad'], inplace=True)
        
        # Concatenar los DataFrames válidos e inválidos
        df_final = pd.concat([df_valido, df_invalido], ignore_index=True)
        
        return df_final
    
    def clean_birken_data(self, df_null: pd.DataFrame) -> pd.DataFrame:
        """Limpia datos de BIRKEN usando diccionario embebido - Lógica exacta del notebook"""
        if self.birken_dict.empty:
            st.error("❌ Diccionario de BIRKEN no disponible")
            return df_null
        
        # Extraer u_estilo de ItemName (lo que está antes del primer '/')
        df_null['u_estilo'] = df_null['ItemName'].str.split('/').str[0]
        
        # Condición 1: que tenga al menos un "/"
        tiene_slash = df_null['ItemName'].str.contains('/')
        
        # Condición 2: que u_estilo tenga al menos 2 caracteres y cumpla formato letra+número
        formato_correcto = (
            df_null['u_estilo'].str.len() >= 2 &
            df_null['u_estilo'].str[0].str.isalpha() &
            df_null['u_estilo'].str[1].str.isnumeric()
        )
        
        # Condición final: válido si cumple ambas
        cond_valido = tiene_slash & formato_correcto
        
        # Separar válidos e inválidos
        df_valido = df_null[cond_valido].copy()
        df_invalido = df_null[~cond_valido].copy()
        
        # Limpiar u_estilo en los inválidos
        df_invalido['u_estilo'] = np.nan
        
        # Para VÁLIDOS: extraer u_descripcion y u_descrip_color
        df_valido['u_descripcion'] = df_valido['ItemName'].str.split('/').str[1]
        df_valido['u_descrip_color'] = df_valido['ItemName'].str.split('/').str[3]
        
        # Merge para válidos
        bk_temp = self.birken_dict[['ItemCode', 'u_coleccion', 'u_genero', 'u_division']].rename(
            columns={
                'u_coleccion': 'u_coleccion_bk',
                'u_genero': 'u_genero_bk',
                'u_division': 'u_division_bk'
            }
        )
        df_valido = df_valido.merge(bk_temp, on='ItemCode', how='left')
        
        # Llenar columnas en df_valido solo si están vacías
        for col in ['u_genero', 'u_coleccion', 'u_division']:
            # Crear columna si no existe
            if col not in df_valido.columns:
                df_valido[col] = np.nan
            
            if f'{col}_bk' in df_valido.columns:
                df_valido[col] = df_valido[f'{col}_bk'].combine_first(df_valido[col])
                df_valido.drop(columns=[f'{col}_bk'], inplace=True)
        
        # Para INVÁLIDOS: merge con todas las columnas
        bk_temp = self.birken_dict[['ItemCode', 'u_coleccion', 'u_genero', 'u_division', 'u_estilo', 'u_descripcion', 'u_descrip_color']].rename(
            columns={
                'u_coleccion': 'u_coleccion_bk',
                'u_genero': 'u_genero_bk',
                'u_division': 'u_division_bk',
                'u_estilo': 'u_estilo_bk',
                'u_descripcion': 'u_descripcion_bk',
                'u_descrip_color': 'u_descrip_color_bk'
            }
        )
        df_invalido = df_invalido.merge(bk_temp, on='ItemCode', how='left')
        
        # Llenar columnas en df_invalido solo si están vacías
        for col in ['u_genero', 'u_coleccion', 'u_division', 'u_estilo', 'u_descripcion', 'u_descrip_color']:
            # Crear columna si no existe
            if col not in df_invalido.columns:
                df_invalido[col] = np.nan
            
            if f'{col}_bk' in df_invalido.columns:
                df_invalido[col] = df_invalido[f'{col}_bk'].combine_first(df_invalido[col])
                df_invalido.drop(columns=[f'{col}_bk'], inplace=True)
        
        # Concatenar los dos dataframes
        df_final = pd.concat([df_valido, df_invalido], ignore_index=True)
        
        return df_final
    
    def clean_new_era_data(self, df_null: pd.DataFrame) -> pd.DataFrame:
        """Limpia datos de NEW ERA usando diccionario combinado L1-L7"""
        if self.new_era_dict.empty:
            st.error("❌ Diccionario de NEW ERA no disponible")
            return df_null
        
        # Extraer componentes del ItemName según lógica del notebook
        df_null['U_Estilo'] = df_null['ItemName'].str.split('/').str[0]
        df_null['U_Descripcion'] = (
            df_null['ItemName']
            .str.replace('\\s+', ' ', regex=True)  # Reemplazar múltiples espacios
            .str.strip()  # Eliminar espacios al inicio y final
            .str.split('/')  # Dividir por '/'
            .str[1]  # Tomar la segunda parte
            .str.strip()  # Eliminar espacios del resultado
        )
        df_null['U_Talla'] = df_null['ItemName'].str.split('/').str[2]
        
        # Columnas a completar según notebook
        columnas_completar = ['U_Estilo', 'U_Silueta', 'U_Team', 'U_Descrip_Color', 'U_Segmento',
                             'U_Liga', 'U_Coleccion_NE', 'U_Genero', 'U_Descripcion', 'U_Temporalidad']
        
        # Columnas adicionales a mantener
        columnas_extra = ['ItemCode', 'Empresa', 'ItemName', 'U_Talla']
        
        # Asegurar que se trabaja con las columnas correctas
        df_reference = self.new_era_dict[columnas_completar] if not self.new_era_dict.empty else pd.DataFrame()
        
        # Mantener solo columnas existentes en df_null
        columnas_existentes = [col for col in columnas_completar + columnas_extra if col in df_null.columns]
        df_null = df_null[columnas_existentes]
        
        # Convertir U_Estilo al mismo tipo de dato
        df_null['U_Estilo'] = df_null['U_Estilo'].astype(str)
        if not df_reference.empty:
            df_reference['U_Estilo'] = df_reference['U_Estilo'].astype(str)
        
        # Crear diccionario de referencia sin duplicados por U_Estilo
        if not df_reference.empty:
            df_reference_clean = df_reference.drop_duplicates('U_Estilo')
            
            # Versión optimizada con merge
            temp_df = df_null[columnas_extra + ['U_Estilo']].merge(
                df_reference_clean, 
                on='U_Estilo', 
                how='left',
                suffixes=('', '_ref')
            )
            
            # Completar valores nulos (empezar desde 1 para omitir U_Estilo)
            for col in columnas_completar[1:]:
                if col in temp_df.columns and col in df_null.columns:
                    mask = df_null[col].isnull()
                    df_null.loc[mask, col] = temp_df.loc[mask, col]
        
        # Aplicar validación de licencias de equipos
        if 'U_Liga' in df_null.columns and 'U_Team' in df_null.columns:
            registros_en_blanco = df_null[df_null['U_Liga'].isna() | (df_null['U_Liga'] == '')]
            for index, row in registros_en_blanco.iterrows():
                equipo = row['U_Team'] if pd.notna(row['U_Team']) else ''
                if equipo in self.team_licenses:
                    df_null.at[index, 'U_Liga'] = self.team_licenses[equipo]
        
        return df_null
    
    def clean_pb_data(self, df_null: pd.DataFrame) -> pd.DataFrame:
        """Limpia datos de PB usando diccionario embebido - Lógica exacta del notebook"""
        if self.pb_dict.empty:
            st.error("❌ Diccionario de PB no disponible")
            return df_null
        
        def validar_y_extraer_u_estilo(itemname):
            try:
                u_estilo = itemname.split('/')[0]
                
                if len(u_estilo) >= 2:
                    primer_caracter = u_estilo[0]
                    segundo_caracter = u_estilo[1]
                    es_valido = primer_caracter.isalpha() and segundo_caracter.isdigit()
                else:
                    es_valido = False
                    primer_caracter = None
                    segundo_caracter = None
                
                return pd.Series([u_estilo, primer_caracter, segundo_caracter, es_valido])
            except:
                return pd.Series([None, None, None, False])
        
        # Aplicar al DataFrame
        df_null[['u_estilo', 'letra', 'numero', 'es_valido']] = df_null['ItemName'].apply(validar_y_extraer_u_estilo)
        
        # Dividir DataFrames
        df_validos = df_null[df_null['es_valido']].copy()
        df_invalidos = df_null[~df_null['es_valido']].copy()
        
        # Drop columnas temporales
        df_validos.drop(columns=['es_valido', 'letra', 'numero'], inplace=True)
        df_invalidos.drop(columns=['es_valido', 'letra', 'numero'], inplace=True)
        
        # Para VÁLIDOS: extraer u_descripcion y u_descrip_color
        df_validos['u_descripcion'] = df_validos['ItemName'].str.split('/').str[1]
        df_validos['u_descrip_color'] = df_validos['ItemName'].str.split('/').str[3]
        
        # Merge para válidos
        df_dict_temp = self.pb_dict[['ItemCode', 'Empresa', 'u_genero', 'u_prenda', 'u_subprenda', 'u_temporalidad']].rename(
            columns={
                'u_genero': 'u_genero_dict',
                'u_prenda': 'u_prenda_dict',
                'u_subprenda': 'u_subprenda_dict',
                'u_temporalidad': 'u_temporalidad_dict'
            }
        )
        df_validos = df_validos.merge(df_dict_temp, on=['ItemCode', 'Empresa'], how='left')
        
        # Llenar columnas en df_validos solo si están vacías
        for col in ['u_genero', 'u_prenda', 'u_subprenda', 'u_temporalidad']:
            # Crear columna si no existe
            if col not in df_validos.columns:
                df_validos[col] = np.nan
            
            col_dict = f"{col}_dict"
            if col_dict in df_validos.columns:
                df_validos[col] = df_validos[col_dict].combine_first(df_validos[col])
                df_validos.drop(columns=[col_dict], inplace=True)
        
        # Para INVÁLIDOS: limpiar u_estilo y hacer merge completo
        df_invalidos['u_estilo'] = np.nan
        
        df_dict_temp = self.pb_dict[['ItemCode', 'Empresa', 'u_genero', 'u_prenda', 'u_subprenda', 'u_temporalidad', 'u_estilo', 'u_descripcion', 'u_descrip_color']].rename(
            columns={
                'u_genero': 'u_genero_dict',
                'u_prenda': 'u_prenda_dict',
                'u_subprenda': 'u_subprenda_dict',
                'u_temporalidad': 'u_temporalidad_dict',
                'u_estilo': 'u_estilo_dict',
                'u_descripcion': 'u_descripcion_dict',
                'u_descrip_color': 'u_descrip_color_dict'
            }
        )
        df_invalidos = df_invalidos.merge(df_dict_temp, on=['ItemCode', 'Empresa'], how='left')
        
        # Llenar columnas en df_invalidos solo si están vacías
        for col in ['u_genero', 'u_prenda', 'u_subprenda', 'u_temporalidad', 'u_estilo', 'u_descripcion', 'u_descrip_color']:
            # Crear columna si no existe
            if col not in df_invalidos.columns:
                df_invalidos[col] = np.nan
            
            col_dict = f"{col}_dict"
            if col_dict in df_invalidos.columns:
                df_invalidos[col] = df_invalidos[col_dict].combine_first(df_invalidos[col])
                df_invalidos.drop(columns=[col_dict], inplace=True)
        
        # Concatenar los dos dataframes
        df_final = pd.concat([df_validos, df_invalidos], ignore_index=True)
        
        return df_final
    
    def clean_skechers_data(self, df_null: pd.DataFrame) -> pd.DataFrame:
        """Limpia datos de SKECHERS usando diccionario embebido - Lógica exacta del notebook"""
        if self.skechers_dict.empty:
            st.error("❌ Diccionario de SKECHERS no disponible")
            return df_null
        
        # Extraer U_Estilo de ItemName (lo que está antes del primer '/')
        df_null['U_Estilo'] = df_null['ItemName'].str.split('/').str[0]
        
        # Condición 1: que tenga al menos un "/"
        tiene_slash = df_null['ItemName'].str.contains('/')
        
        # Condición 2: que u_estilo tenga al menos 2 caracteres y empiece con letra o número
        formato_correcto = (
            df_null['U_Estilo'].str.len() >= 2
        ) & (
            df_null['U_Estilo'].str[0].str.isalnum()
        )
        
        # Condición final: válido si cumple ambas
        cond_valido = tiene_slash & formato_correcto
        
        # Separar válidos e inválidos
        df_valido = df_null[cond_valido].copy()
        df_invalido = df_null[~cond_valido].copy()
        
        # Limpiar u_estilo en los inválidos
        df_invalido['U_Estilo'] = np.nan
        
        # Para VÁLIDOS: extraer U_Descripcion y limpiar "americana"
        df_valido['U_Descripcion'] = df_valido['ItemName'].str.split('/').str[1]
        
        # Eliminar "Americana" seguido de número (entero o decimal), con o sin espacio
        df_valido['U_Descripcion'] = df_valido['U_Descripcion'].str.replace(
            r'(?i)americana\s*\d+(?:\.\d+)?', '', regex=True
        )
        
        # Merge para válidos
        df_temp = self.skechers_dict[['ItemCode', 'Empresa', 'U_Genero', 'U_Suela', 'U_Descrip_Color',
                                    'U_Segmentacion_SK', 'U_Division', 'U_Temporalidad']].copy()
        df_temp.rename(columns={
            'U_Genero': 'U_Genero_sk',
            'U_Suela': 'U_Suela_sk',
            'U_Descrip_Color': 'U_Descrip_Color_sk',
            'U_Segmentacion_SK': 'U_Segmentacion_SK_sk',
            'U_Division': 'U_Division_sk',
            'U_Temporalidad': 'U_Temporalidad_sk'
        }, inplace=True)
        df_valido = df_valido.merge(df_temp, on=['ItemCode', 'Empresa'], how='left')
        
        # Llenar columnas en df_valido solo si están vacías
        columnas = ['U_Genero', 'U_Suela', 'U_Descrip_Color',
                   'U_Segmentacion_SK', 'U_Division', 'U_Temporalidad']
        for col in columnas:
            # Crear columna si no existe
            if col not in df_valido.columns:
                df_valido[col] = np.nan
            
            col_sk = f'{col}_sk'
            if col_sk in df_valido.columns:
                df_valido[col] = df_valido[col].combine_first(df_valido[col_sk])
                df_valido.drop(columns=[col_sk], inplace=True)
        
        # Para INVÁLIDOS: merge con todas las columnas
        df_temp = self.skechers_dict[['ItemCode', 'Empresa', 'U_Estilo', 'U_Genero', 'U_Suela', 'U_Descrip_Color',
                                    'U_Segmentacion_SK', 'U_Division', 'U_Temporalidad', 'U_Descripcion']].copy()
        df_temp.rename(columns={
            'U_Estilo': 'U_Estilo_sk',
            'U_Genero': 'U_Genero_sk',
            'U_Suela': 'U_Suela_sk',
            'U_Descrip_Color': 'U_Descrip_Color_sk',
            'U_Segmentacion_SK': 'U_Segmentacion_SK_sk',
            'U_Division': 'U_Division_sk',
            'U_Temporalidad': 'U_Temporalidad_sk',
            'U_Descripcion': 'U_Descripcion_sk'
        }, inplace=True)
        df_invalido = df_invalido.merge(df_temp, on=['ItemCode', 'Empresa'], how='left')
        
        # Llenar columnas en df_invalido solo si están vacías
        columnas = ['U_Estilo', 'U_Genero', 'U_Suela', 'U_Descrip_Color',
                   'U_Segmentacion_SK', 'U_Division', 'U_Temporalidad', 'U_Descripcion']
        for col in columnas:
            # Crear columna si no existe
            if col not in df_invalido.columns:
                df_invalido[col] = np.nan
            
            col_sk = f'{col}_sk'
            if col_sk in df_invalido.columns:
                df_invalido[col] = df_invalido[col].combine_first(df_invalido[col_sk])
                df_invalido.drop(columns=[col_sk], inplace=True)
        
        # Concatenar los DataFrames válidos e inválidos
        df_final = pd.concat([df_valido, df_invalido], ignore_index=True)
        
        return df_final
    
    def clean_ch_data(self, df_null: pd.DataFrame) -> pd.DataFrame:
        """Limpia datos de CH usando diccionario embebido - Lógica del notebook"""
        if self.ch_dict.empty:
            st.error("❌ Diccionario de CH no disponible")
            return df_null
        
        # 1. Extraer U_Estilo de ItemName (lo que está antes del primer '/')
        df_null['U_Estilo'] = df_null['ItemName'].str.split('/').str[0]
        
        # 2. Asignar U_Genero y U_Categoria basado en el primer carácter
        conditions = [
            df_null['U_Estilo'].str.startswith('F'),
            df_null['U_Estilo'].str.startswith('W'),
            df_null['U_Estilo'].str.startswith('C'),
            df_null['U_Estilo'].str.startswith('U')
        ]
        choices = ['MACC', 'WFW', 'MFW', 'WACC']
        df_null['U_Genero'] = np.select(conditions, choices, default='')
        df_null['U_Categoria'] = df_null['U_Genero']  # Mismo valor que U_Genero
        
        # 3. Asignar U_Segmento según reglas
        df_null['U_Segmento'] = np.where(
            df_null['U_Genero'].isin(['WFW', 'MFW']),
            'FOOTWEAR',
            np.where(df_null['U_Genero'].isin(['MACC', 'WACC']), 'ACCESSORIES', '')
        )
        
        # 4. Extraer U_Talla de ItemName (posición 2)
        df_null['U_Talla'] = df_null['ItemName'].str.split('/').str[2]
        
        # 5. Merge con diccionario CH para completar información
        # Crear columnas faltantes si no existen
        for col in ['U_Descripcion', 'U_Segmentacion_SK', 'U_Zone', 'U_Descrip_Color']:
            if col not in df_null.columns:
                df_null[col] = np.nan
        
        # Hacer merge con diccionario CH
        ch_temp = self.ch_dict[['U_Estilo', 'U_Descripcion', 'U_Segmentacion_SK', 'U_Zone', 'U_Descrip_Color']].rename(
            columns={
                'U_Descripcion': 'U_Descripcion_ch',
                'U_Segmentacion_SK': 'U_Segmentacion_SK_ch',
                'U_Zone': 'U_Zone_ch',
                'U_Descrip_Color': 'U_Descrip_Color_ch'
            }
        )
        df_null = df_null.merge(ch_temp, on='U_Estilo', how='left')
        
        # Llenar columnas faltantes con datos del diccionario
        for col in ['U_Descripcion', 'U_Segmentacion_SK', 'U_Zone', 'U_Descrip_Color']:
            col_ch = f'{col}_ch'
            if col_ch in df_null.columns:
                df_null[col] = df_null[col_ch].combine_first(df_null[col])
                df_null.drop(columns=[col_ch], inplace=True)
        
        # 6. Ordenar columnas finales
        columnas_finales = [
            'ItemName', 'ItemCode', 'Empresa', 'U_Estilo', 'U_Genero', 'U_Categoria',
            'U_Segmento', 'U_Descripcion', 'U_Descrip_Color', 'U_Segmentacion_SK', 'U_Zone', 'U_Talla'
        ]
        
        # Agregar columnas que falten
        for col in columnas_finales:
            if col not in df_null.columns:
                df_null[col] = np.nan
        
        df_final = df_null[columnas_finales]
        
        return df_final

def main():
    st.set_page_config(
        page_title="BOTS Dashboard - Limpieza de Datos",
        page_icon="🤖",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # CSS limpio y minimalista
    st.markdown("""
    <style>
    .main {
        padding: 1rem;
        background-color: #ffffff;
    }
    .main-header {
        background-color: #2c3e50;
        padding: 1.5rem;
        border-radius: 8px;
        margin-bottom: 1.5rem;
        text-align: center;
        color: white;
        border: 1px solid #34495e;
    }
    .bot-card {
        background-color: #f8f9fa;
        padding: 1.2rem;
        border-radius: 6px;
        border: 1px solid #dee2e6;
        margin: 0.8rem 0;
    }
    .metric-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 6px;
        text-align: center;
        margin: 0.5rem 0;
        border: 1px solid #e9ecef;
        transition: all 0.3s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.12);
    }
    .stButton > button {
        background-color: #3498db;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 0.6rem 1.2rem;
        font-weight: 500;
        border: 1px solid #2980b9;
    }
    .stButton > button:hover {
        background-color: #2980b9;
    }
    .stButton > button:disabled {
        background-color: #bdc3c7;
        color: #7f8c8d;
    }
    .stSelectbox > div > div {
        background-color: white;
        border: 1px solid #ced4da;
        border-radius: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #f8f9fa;
        border-radius: 4px;
        padding: 0.6rem 1rem;
        margin-right: 0.3rem;
        border: 1px solid #dee2e6;
        color: #495057;
        font-weight: 500;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #e9ecef;
    }
    .stTabs [aria-selected="true"] {
        background-color: #3498db !important;
        color: white !important;
        border: 1px solid #2980b9 !important;
    }
    .stSidebar {
        background-color: #f8f9fa;
        border-right: 1px solid #dee2e6;
    }
    .stSidebar > div {
        background-color: #f8f9fa;
    }
    .stDataFrame {
        background-color: white;
        border-radius: 4px;
        overflow: hidden;
        border: 1px solid #dee2e6;
    }
    .stProgress > div > div {
        background-color: #28a745;
        border-radius: 4px;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 4px;
        padding: 1rem;
        margin: 0.5rem 0;
        color: #155724;
    }
    .info-box {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 4px;
        padding: 1rem;
        margin: 0.5rem 0;
        color: #856404;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f1aeb5;
        border-radius: 4px;
        padding: 1rem;
        margin: 0.5rem 0;
        color: #721c24;
    }
    h1, h2, h3, h4, h5, h6 {
        color: #2c3e50;
        font-weight: 600;
    }
    div[data-testid="stMetricValue"] {
        color: #2c3e50;
        font-weight: 700;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header principal
    st.markdown("""
    <div class="main-header">
        <h1>🤖 BOTS Dashboard</h1>
        <h3>Grupo Disresa</h3>
        <p>Procesamiento automático de datos para diferentes marcas</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Inicializar limpiador
    cleaner = DataCleaner()
    
    # Sidebar mejorado
    with st.sidebar:
        st.markdown("### 🎯 Configuración del Proceso")
        
        bot_type = st.selectbox(
            "Selecciona el BOT:",
            ["ADOLFO", "BIRKEN", "NEW ERA", "PB", "SKECHERS", "CH"],
            help="Cada BOT tiene algoritmos específicos para su marca"
        )
        
        # Información del bot seleccionado
        bot_info = {
            "ADOLFO": {
                "icon": "👔",
                "description": "Procesamiento de datos Adolfo Domínguez",
                "features": ["Extracción de estilos", "Validación letra+número", "Completado de categorías"],
                "dict_size": 0
            },
            "BIRKEN": {
                "icon": "🦶",
                "description": "Procesamiento de datos Birkenstock",
                "features": ["Extracción de descripción", "Validación de colores", "Completado de divisiones"],
                "dict_size": 0
            },
            "NEW ERA": {
                "icon": "🧢",
                "description": "Procesamiento complejo de gorras",
                "features": ["Diccionario de equipos", "Validación deportiva", "Completado temporal"],
                "dict_size": 0
            },
            "PB": {
                "icon": "👕",
                "description": "Procesamiento de moda/ropa",
                "features": ["Validación de prendas", "Extracción de temporalidad", "Completado de géneros"],
                "dict_size": 0
            },
            "SKECHERS": {
                "icon": "👟",
                "description": "Procesamiento de calzado deportivo",
                "features": ["Limpieza de descripciones", "Validación de suelas", "Completado de segmentación"],
                "dict_size": 0
            },
            "CH": {
                "icon": "👠",
                "description": "Procesamiento de Cole Haan",
                "features": ["Categorización automática F/W/C/U", "Segmentación FOOTWEAR/ACCESSORIES", "Diccionarios L1-L6"],
                "dict_size": 0
            }
        }
        
        st.markdown(f"""
        <div class="bot-card">
            <h4>{bot_info[bot_type]['icon']} {bot_type}</h4>
            <p>{bot_info[bot_type]['description']}</p>
            <p><strong>📊 Diccionario:</strong> {bot_info[bot_type]['dict_size']:,} registros</p>
            <ul>
                {''.join([f"<li>{feature}</li>" for feature in bot_info[bot_type]['features']])}
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        st.markdown("### 📋 Instrucciones")
        st.info("""
        1. Sube el archivo CSV con datos sucios
        2. Verifica la información cargada
        3. Ejecuta el proceso de limpieza
        4. Revisa los resultados
        5. Descarga el archivo procesado
        """)
    
    # Contenido principal con tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📤 Subir Archivo", "📊 Estado del Sistema", "📈 Métricas Tiempo Real", "💾 Backup"])
    
    with tab1:
        st.markdown("### 📁 Subir Archivo de Datos Sucios")
        
        uploaded_file = st.file_uploader(
            "Selecciona el archivo CSV con datos sucios",
            type=['csv'],
            help="Archivo con datos incompletos que necesitan limpieza"
        )
        
        if uploaded_file:
            st.markdown(f"""
            <div class="success-box">
                <strong>✅ Archivo cargado:</strong> {uploaded_file.name}<br>
                <strong>Tamaño:</strong> {uploaded_file.size / 1024:.1f} KB
            </div>
            """, unsafe_allow_html=True)
            
            # Botón para procesar
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button("🚀 Procesar Datos", type="primary", use_container_width=True):
                    process_data(bot_type, cleaner, uploaded_file)
        else:
            st.markdown("""
            <div class="info-box">
                <strong>⚠️ Esperando archivo:</strong> Sube un archivo CSV para comenzar
            </div>
            """, unsafe_allow_html=True)
    
    with tab2:
        st.markdown("### 📊 Estado del Sistema")
        
        # Mostrar estado de diccionarios (simplificado para optimización)
        st.markdown("### 📊 Estado de Diccionarios")
        st.info("✅ Los diccionarios se cargan automáticamente cuando seleccionas un bot")
        
        # Mostrar información simplificada de los bots
        st.markdown("""
        <div style="display: flex; flex-wrap: wrap; gap: 10px;">
            <div class="metric-card" style="flex: 1; min-width: 200px;">
                <h4>👔 ADOLFO</h4>
                <h2 style="color: #27ae60;">Disponible</h2>
                <p>Diccionario cargado bajo demanda</p>
            </div>
            <div class="metric-card" style="flex: 1; min-width: 200px;">
                <h4>🦶 BIRKEN</h4>
                <h2 style="color: #27ae60;">Disponible</h2>
                <p>Diccionario cargado bajo demanda</p>
            </div>
            <div class="metric-card" style="flex: 1; min-width: 200px;">
                <h4>🏀 PB</h4>
                <h2 style="color: #27ae60;">Disponible</h2>
                <p>Diccionario cargado bajo demanda</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div style="display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px;">
            <div class="metric-card" style="flex: 1; min-width: 200px;">
                <h4>👟 SKECHERS</h4>
                <h2 style="color: #27ae60;">Disponible</h2>
                <p>Diccionario cargado bajo demanda</p>
            </div>
            <div class="metric-card" style="flex: 1; min-width: 200px;">
                <h4>🧢 NEW ERA</h4>
                <h2 style="color: #27ae60;">Disponible</h2>
                <p>Diccionario cargado bajo demanda</p>
            </div>
            <div class="metric-card" style="flex: 1; min-width: 200px;">
                <h4>👞 CH</h4>
                <h2 style="color: #27ae60;">Disponible</h2>
                <p>Diccionario cargado bajo demanda</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with tab3:
        render_metrics_tab()
    
    with tab4:
        render_backup_tab()

def process_data(bot_type, cleaner, uploaded_file):
    """Procesa los datos según el tipo de bot"""
    try:
        with st.spinner(f"🔄 Procesando datos de {bot_type}..."):
            # Cargar archivo
            df_null = pd.read_csv(uploaded_file, sep=';', low_memory=False)
            
            # Mostrar información de archivo cargado
            st.markdown("### 📂 Archivo Cargado")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <h4>📄 Datos Sucios</h4>
                    <p><strong>Filas:</strong> {df_null.shape[0]:,}</p>
                    <p><strong>Columnas:</strong> {df_null.shape[1]}</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                null_count = df_null.isnull().sum().sum()
                st.markdown(f"""
                <div class="metric-card">
                    <h4>❌ Valores Nulos</h4>
                    <p><strong>Total:</strong> {null_count:,}</p>
                    <p><strong>Porcentaje:</strong> {(null_count / (df_null.shape[0] * df_null.shape[1]) * 100):.1f}%</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                # Obtener info del diccionario
                if bot_type == "ADOLFO":
                    dict_size = len(cleaner.get_adolfo_dict()) if not cleaner.get_adolfo_dict().empty else 0
                elif bot_type == "BIRKEN":
                    dict_size = len(cleaner.get_birken_dict()) if not cleaner.get_birken_dict().empty else 0
                elif bot_type == "NEW ERA":
                    dict_size = len(cleaner.get_new_era_dict()) if not cleaner.get_new_era_dict().empty else 0
                elif bot_type == "PB":
                    dict_size = len(cleaner.get_pb_dict()) if not cleaner.get_pb_dict().empty else 0
                elif bot_type == "SKECHERS":
                    dict_size = len(cleaner.get_skechers_dict()) if not cleaner.get_skechers_dict().empty else 0
                elif bot_type == "CH":
                    dict_size = len(cleaner.get_ch_dict()) if not cleaner.get_ch_dict().empty else 0
                
                st.markdown(f"""
                <div class="metric-card">
                    <h4>📚 Diccionario</h4>
                    <p><strong>Registros:</strong> {dict_size:,}</p>
                    <p><strong>Estado:</strong> {'✅ Disponible' if dict_size > 0 else '❌ No disponible'}</p>
                </div>
                """, unsafe_allow_html=True)
            
            # Mostrar muestra de datos sucios
            st.markdown("**Vista previa de datos sucios:**")
            st.dataframe(df_null.head(5), use_container_width=True)
            
            st.markdown("---")
            
            # Proceso de limpieza
            st.markdown("### 🔧 Proceso de Limpieza")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text("Iniciando proceso de limpieza...")
            progress_bar.progress(25)
            
            # Aplicar limpieza según el bot
            if bot_type == "ADOLFO":
                status_text.text("Aplicando lógica de limpieza ADOLFO...")
                df_final = cleaner.clean_adolfo_data(df_null)
            elif bot_type == "BIRKEN":
                status_text.text("Aplicando lógica de limpieza BIRKEN...")
                df_final = cleaner.clean_birken_data(df_null)
            elif bot_type == "NEW ERA":
                status_text.text("Aplicando lógica de limpieza NEW ERA...")
                df_final = cleaner.clean_new_era_data(df_null)
            elif bot_type == "PB":
                status_text.text("Aplicando lógica de limpieza PB...")
                df_final = cleaner.clean_pb_data(df_null)
            elif bot_type == "SKECHERS":
                status_text.text("Aplicando lógica de limpieza SKECHERS...")
                df_final = cleaner.clean_skechers_data(df_null)
            elif bot_type == "CH":
                status_text.text("Aplicando lógica de limpieza CH...")
                df_final = cleaner.clean_ch_data(df_null)
            
            progress_bar.progress(75)
            status_text.text("Finalizando proceso...")
            progress_bar.progress(100)
            status_text.text("¡Proceso completado exitosamente!")
            
            # Mostrar resultados con diseño mejorado
            st.markdown("### 🎯 Resultados del Proceso")
            
            # Métricas principales
            metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
            
            with metrics_col1:
                st.markdown(f"""
                <div class="metric-card">
                    <h4>📊 Filas Totales</h4>
                    <h2 style="color: #3498db;">{df_final.shape[0]:,}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with metrics_col2:
                st.markdown(f"""
                <div class="metric-card">
                    <h4>📋 Columnas</h4>
                    <h2 style="color: #9b59b6;">{df_final.shape[1]}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            with metrics_col3:
                # Contar valores no nulos en columna clave
                key_col = get_key_column(bot_type)
                if key_col in df_final.columns:
                    valid_count = df_final[key_col].notna().sum()
                    valid_pct = (valid_count / df_final.shape[0]) * 100
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>✅ Registros Válidos</h4>
                        <h2 style="color: #27ae60;">{valid_count:,}</h2>
                        <p style="color: #27ae60;">({valid_pct:.1f}%)</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4>✅ Registros Válidos</h4>
                        <h2 style="color: #95a5a6;">N/A</h2>
                    </div>
                    """, unsafe_allow_html=True)
            
            with metrics_col4:
                # Calcular registros con información completa
                complete_count = df_final.dropna().shape[0]
                complete_pct = (complete_count / df_final.shape[0]) * 100
                st.markdown(f"""
                <div class="metric-card">
                    <h4>🔍 Completos</h4>
                    <h2 style="color: #f39c12;">{complete_count:,}</h2>
                    <p style="color: #f39c12;">({complete_pct:.1f}%)</p>
                </div>
                """, unsafe_allow_html=True)
            
            # Vista previa del resultado
            st.markdown("### 📋 Vista Previa del Resultado")
            
            # Tabs para diferentes vistas
            tab1, tab2, tab3 = st.tabs(["📊 Primeros 10 registros", "🔍 Muestra aleatoria", "📈 Resumen estadístico"])
            
            with tab1:
                st.dataframe(df_final.head(10), use_container_width=True)
            
            with tab2:
                if df_final.shape[0] > 10:
                    st.dataframe(df_final.sample(min(10, df_final.shape[0])), use_container_width=True)
                else:
                    st.info("El dataset es muy pequeño para mostrar una muestra aleatoria")
            
            with tab3:
                st.dataframe(df_final.describe(include='all'), use_container_width=True)
            
            # Botón de descarga mejorado
            st.markdown("### 📥 Descargar Resultado")
            
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col2:
                csv = df_final.to_csv(index=False, sep=';', encoding='utf-8')
                
                st.download_button(
                    label="📥 Descargar Archivo Limpio",
                    data=csv,
                    file_name=f"{bot_type.lower()}_limpio_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    type="primary",
                    use_container_width=True
                )
                
                st.info(f"📊 Archivo generado: {len(csv)} caracteres | Separador: ; | Encoding: UTF-8")
                
                st.markdown("---")
                
                # Opción de descarga en formato plantilla
                st.markdown("### 📋 Formato Plantilla")
                
                # Convertir a formato plantilla
                df_plantilla = convert_to_plantilla_format(df_final, bot_type)
                
                if not df_plantilla.empty:
                    st.markdown(f"**Vista previa formato plantilla:** ({len(df_plantilla)} registros)")
                    st.dataframe(df_plantilla.head(10), use_container_width=True)
                    
                    # Generar CSV para plantilla
                    csv_plantilla = df_plantilla.to_csv(index=False, sep=';', encoding='utf-8')
                    
                    st.download_button(
                        label="📋 Descargar en Formato Plantilla",
                        data=csv_plantilla,
                        file_name=f"{bot_type.lower()}_plantilla_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        type="secondary",
                        use_container_width=True
                    )
                    
                    # Mostrar estadísticas del formato plantilla
                    st.info(f"""
                    📋 **Formato Plantilla:**
                    - **Registros:** {len(df_plantilla):,}
                    - **Columnas:** Pais, DB, COLUMNA, Codigo_SAP, VALOR
                    - **DB:** SBO_00_DISRESA
                    - **Separador:** ; | **Encoding:** UTF-8
                    """)
                else:
                    st.warning("⚠️ No se pudo generar el formato plantilla")
        
        st.success(f"✅ Proceso completado exitosamente! Se procesaron {df_final.shape[0]:,} registros.")
        
    except Exception as e:
        st.markdown(f"""
        <div class="error-box">
            <strong>❌ Error al procesar los datos:</strong> {str(e)}
        </div>
        """, unsafe_allow_html=True)
        
        with st.expander("🔧 Detalles del Error"):
            st.exception(e)

def get_key_column(bot_type):
    """Retorna la columna clave para cada tipo de bot"""
    key_columns = {
        "ADOLFO": "u_estilo",
        "BIRKEN": "u_estilo", 
        "NEW ERA": "U_Estilo",
        "PB": "u_estilo",
        "SKECHERS": "U_Estilo",
        "CH": "U_Estilo"
    }
    return key_columns.get(bot_type, "u_estilo")

def convert_to_plantilla_format(df_cleaned, bot_type):
    """Convierte los datos limpios al formato de plantilla.xlsx"""
    try:
        # Estructura base de la plantilla
        plantilla_data = []
        
        # Mapeo de columnas por bot (excluyendo ItemName, ItemCode, Empresa)
        column_mapping = {
            "ADOLFO": ['u_estilo', 'u_categoria', 'u_genero', 'u_familia', 'u_descrip_color'],
            "BIRKEN": ['u_estilo', 'u_coleccion', 'u_genero', 'u_division', 'u_descripcion', 'u_descrip_color'],
            "NEW ERA": ['U_Estilo', 'U_Silueta', 'U_Team', 'U_Descrip_Color', 'U_Segmento', 'U_Liga', 'U_Coleccion_NE', 'U_Genero', 'U_Descripcion', 'U_Temporalidad', 'U_Talla'],
            "PB": ['u_estilo', 'u_genero', 'u_prenda', 'u_subprenda', 'u_temporalidad', 'u_descripcion', 'u_descrip_color'],
            "SKECHERS": ['U_Estilo', 'U_Genero', 'U_Suela', 'U_Descrip_Color', 'U_Segmentacion_SK', 'U_Division', 'U_Temporalidad', 'U_Descripcion'],
            "CH": ['U_Estilo', 'U_Genero', 'U_Categoria', 'U_Segmento', 'U_Descripcion', 'U_Descrip_Color', 'U_Segmentacion_SK', 'U_Zone', 'U_Talla']
        }
        
        columns_to_process = column_mapping.get(bot_type, [])
        
        # Iterar por cada fila y cada columna
        for index, row in df_cleaned.iterrows():
            codigo_sap = row.get('ItemCode', '')
            
            for columna in columns_to_process:
                if columna in df_cleaned.columns:
                    valor = row.get(columna, '')
                    
                    # Solo agregar si el valor no está vacío
                    if pd.notna(valor) and str(valor).strip() != '' and str(valor).strip() != 'nan':
                        plantilla_data.append({
                            'Pais': None,  # Como en la plantilla original
                            'DB': row.get('Empresa', ''),  # Usar exactamente el nombre de empresa del archivo sucio
                            'COLUMNA': columna,
                            'Codigo_SAP': codigo_sap,
                            'VALOR': str(valor).strip()
                        })
        
        # Crear DataFrame con formato de plantilla
        df_plantilla = pd.DataFrame(plantilla_data)
        
        return df_plantilla
        
    except Exception as e:
        st.error(f"Error convirtiendo al formato plantilla: {str(e)}")
        return pd.DataFrame()

# === SISTEMA DE MÉTRICAS EN TIEMPO REAL ===
def generate_metrics():
    """Genera métricas simuladas para demostración"""
    return {
        "archivos_procesados": random.randint(45, 65),
        "registros_totales": random.randint(8500, 12000),
        "registros_validos": random.randint(7800, 11500),
        "tiempo_promedio": round(random.uniform(2.1, 4.8), 2),
        "cpu_usage": random.randint(25, 85),
        "memoria_uso": random.randint(40, 90),
        "errores_minuto": random.randint(0, 5),
        "throughput": random.randint(150, 300)
    }

def generate_bot_activity():
    """Genera actividad simulada de bots"""
    bots = ["ADOLFO", "BIRKEN", "NEW ERA", "PB", "SKECHERS", "CH"]
    activity = []
    for bot in bots:
        activity.append({
            "Bot": bot,
            "Activo": random.choice([True, False]),
            "Archivos": random.randint(5, 15),
            "Último_Proceso": datetime.now() - timedelta(minutes=random.randint(1, 120)),
            "Status": random.choice(["OK", "WARNING", "ERROR"]),
            "Rendimiento": random.randint(70, 100)
        })
    return activity

def render_metrics_tab():
    """Renderiza la pestaña de métricas en tiempo real"""
    st.markdown("### 📈 Métricas en Tiempo Real")
    
    # Controles
    col1, col2 = st.columns(2)
    with col1:
        auto_refresh = st.checkbox("🔄 Auto-refresh (10s)", value=False)
    with col2:
        if st.button("🔄 Actualizar Ahora"):
            st.rerun()
    
    # Generar métricas
    metrics = generate_metrics()
    
    # Timestamp actual
    st.markdown(f"**Última actualización:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Métricas principales
    st.subheader("🎯 Métricas Principales")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Archivos Procesados",
            metrics['archivos_procesados'],
            delta=f"+{random.randint(1, 5)} última hora"
        )
    
    with col2:
        st.metric(
            "Registros Totales",
            f"{metrics['registros_totales']:,}",
            delta=f"+{random.randint(50, 200)} tiempo real"
        )
    
    with col3:
        validez = round((metrics['registros_validos'] / metrics['registros_totales']) * 100, 1)
        st.metric(
            "Registros Válidos",
            f"{validez}%",
            delta=f"{validez - 85:.1f}% vs objetivo"
        )
    
    with col4:
        st.metric(
            "Tiempo Promedio",
            f"{metrics['tiempo_promedio']}s",
            delta=f"{'↓' if metrics['tiempo_promedio'] < 3 else '↑'} vs baseline"
        )
    
    # Alertas del sistema
    st.subheader("🚨 Alertas del Sistema")
    
    alert_count = 0
    if metrics['errores_minuto'] > 3:
        st.error(f"⚠️ ALERTA: {metrics['errores_minuto']} errores en el último minuto")
        alert_count += 1
    
    if metrics['cpu_usage'] > 80:
        st.warning(f"🔥 CPU ALTO: Uso de CPU al {metrics['cpu_usage']}%")
        alert_count += 1
    
    if metrics['memoria_uso'] > 85:
        st.warning(f"💾 MEMORIA ALTA: Uso de memoria al {metrics['memoria_uso']}%")
        alert_count += 1
    
    if alert_count == 0:
        st.success("✅ SISTEMA SALUDABLE: Todos los indicadores normales")
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Rendimiento del Sistema")
        timestamps = [datetime.now() - timedelta(minutes=i) for i in range(10, 0, -1)]
        cpu_data = [random.randint(20, 90) for _ in range(10)]
        memory_data = [random.randint(30, 95) for _ in range(10)]
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=timestamps, y=cpu_data,
            mode='lines+markers',
            name='CPU %',
            line=dict(color='#ff6b6b', width=3)
        ))
        fig.add_trace(go.Scatter(
            x=timestamps, y=memory_data,
            mode='lines+markers',
            name='Memoria %',
            line=dict(color='#4ecdc4', width=3)
        ))
        fig.update_layout(
            title="Uso de Recursos (Últimos 10 minutos)",
            xaxis_title="Tiempo",
            yaxis_title="Porcentaje",
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🔄 Throughput por Bot")
        bot_names = ["ADOLFO", "BIRKEN", "NEW ERA", "PB", "SKECHERS", "CH"]
        throughput_data = [random.randint(10, 50) for _ in range(6)]
        
        fig = px.bar(
            x=bot_names, y=throughput_data,
            title="Registros/Minuto por Bot",
            color=throughput_data,
            color_continuous_scale="viridis"
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    # Estado de los bots
    st.subheader("🤖 Estado de los Bots")
    df_bots = pd.DataFrame(generate_bot_activity())
    st.dataframe(df_bots, use_container_width=True)
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(10)
        st.rerun()

# === SISTEMA DE BACKUP ===
class BackupSystem:
    def __init__(self):
        # Disable backup system for Streamlit deployment
        self.backup_enabled = False
        self.backup_path = "./BACKUPS"
        self.bot_paths = {}
        self.ensure_backup_structure()
    
    def ensure_backup_structure(self):
        """Crea estructura de carpetas de backup"""
        if not self.backup_enabled:
            return
        try:
            Path(self.backup_path).mkdir(exist_ok=True)
            for folder in ['daily', 'weekly', 'monthly', 'manual']:
                Path(f"{self.backup_path}/{folder}").mkdir(exist_ok=True)
        except:
            self.backup_enabled = False
    
    def create_backup(self, backup_type="manual", specific_bot=None):
        """Crea backup completo o de bot específico"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if specific_bot:
            backup_name = f"{specific_bot}_{backup_type}_{timestamp}"
        else:
            backup_name = f"full_{backup_type}_{timestamp}"
        
        backup_folder = f"{self.backup_path}/{backup_type}/{backup_name}"
        Path(backup_folder).mkdir(parents=True, exist_ok=True)
        
        metadata = {
            "backup_name": backup_name,
            "backup_type": backup_type,
            "timestamp": timestamp,
            "created_at": datetime.now().isoformat(),
            "specific_bot": specific_bot,
            "files_backed_up": 0,
            "total_size": 0,
            "status": "completed"
        }
        
        try:
            # Backup de diccionarios
            dict_folder = f"{backup_folder}/dictionaries"
            Path(dict_folder).mkdir(exist_ok=True)
            
            bots_to_backup = [specific_bot] if specific_bot else self.bot_paths.keys()
            
            for bot_name in bots_to_backup:
                bot_path = self.bot_paths[bot_name]
                if os.path.exists(bot_path):
                    bot_backup_path = f"{dict_folder}/{bot_name}"
                    shutil.copytree(bot_path, bot_backup_path)
                    
                    # Contar archivos y tamaño
                    for root, dirs, files in os.walk(bot_backup_path):
                        for file in files:
                            file_path = os.path.join(root, file)
                            metadata["files_backed_up"] += 1
                            metadata["total_size"] += os.path.getsize(file_path)
            
            # Comprimir backup
            zip_path = f"{backup_folder}.zip"
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(backup_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, backup_folder)
                        zipf.write(file_path, arcname)
            
            # Eliminar carpeta original
            shutil.rmtree(backup_folder)
            metadata["compressed"] = True
            metadata["backup_file"] = zip_path
            
            # Guardar metadata
            with open(f"{backup_folder}_metadata.json", 'w') as f:
                json.dump(metadata, f, indent=2)
            
            return metadata
            
        except Exception as e:
            metadata["status"] = "failed"
            metadata["error"] = str(e)
            return metadata
    
    def get_backup_list(self):
        """Lista todos los backups disponibles"""
        backups = []
        
        for backup_type in ['daily', 'weekly', 'monthly', 'manual']:
            backup_folder = f"{self.backup_path}/{backup_type}"
            if os.path.exists(backup_folder):
                for item in os.listdir(backup_folder):
                    if item.endswith('_metadata.json'):
                        metadata_path = os.path.join(backup_folder, item)
                        try:
                            with open(metadata_path, 'r') as f:
                                metadata = json.load(f)
                                backups.append(metadata)
                        except:
                            continue
        
        return sorted(backups, key=lambda x: x['created_at'], reverse=True)

def render_backup_tab():
    """Renderiza la pestaña de backup"""
    st.markdown("### 💾 Sistema de Backup")
    
    st.info("🚧 Sistema de backup deshabilitado en Streamlit Cloud")
    return
    
    # Sub-tabs para backup
    sub_tab1, sub_tab2, sub_tab3 = st.tabs(["📋 Estado", "🔄 Crear", "📂 Restaurar"])
    
    with sub_tab1:
        st.subheader("📊 Estado del Sistema de Backup")
        
        backups = backup_system.get_backup_list()
        
        # Métricas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Backups Totales", len(backups))
        with col2:
            successful = len([b for b in backups if b['status'] == 'completed'])
            st.metric("Backups Exitosos", successful)
        with col3:
            if backups:
                last_backup = max(backups, key=lambda x: x['created_at'])
                last_date = datetime.fromisoformat(last_backup['created_at'])
                days_ago = (datetime.now() - last_date).days
                st.metric("Último Backup", f"hace {days_ago} días")
            else:
                st.metric("Último Backup", "Nunca")
        
        # Lista de backups
        st.subheader("🕒 Backups Recientes")
        if backups:
            df_backups = pd.DataFrame(backups[:5])
            st.dataframe(df_backups[['backup_name', 'backup_type', 'created_at', 'status']])
        else:
            st.info("No hay backups disponibles")
    
    with sub_tab2:
        st.subheader("🔄 Crear Nuevo Backup")
        
        col1, col2 = st.columns(2)
        with col1:
            backup_type = st.selectbox("Tipo de Backup", ["manual", "daily", "weekly", "monthly"])
        with col2:
            specific_bot = st.selectbox("Bot Específico", ["Todos"] + list(backup_system.bot_paths.keys()))
        
        if st.button("🚀 Crear Backup"):
            with st.spinner("Creando backup..."):
                bot_param = None if specific_bot == "Todos" else specific_bot
                result = backup_system.create_backup(backup_type, bot_param)
                
                if result['status'] == 'completed':
                    st.success(f"✅ Backup creado: {result['backup_name']}")
                    st.info(f"📦 {result['files_backed_up']} archivos, {result['total_size']/(1024*1024):.1f} MB")
                else:
                    st.error(f"❌ Error: {result.get('error', 'Error desconocido')}")
    
    with sub_tab3:
        st.subheader("📂 Restaurar Backup")
        
        available_backups = backup_system.get_backup_list()
        if available_backups:
            backup_options = [f"{b['backup_name']} ({b['created_at'][:16]})" for b in available_backups]
            selected = st.selectbox("Seleccionar Backup", backup_options)
            
            if selected:
                backup_index = backup_options.index(selected)
                backup_info = available_backups[backup_index]
                
                st.json(backup_info)
                
                if st.button("🔄 Restaurar"):
                    st.warning("⚠️ Función de restauración en desarrollo")
        else:
            st.info("No hay backups disponibles para restaurar")

if __name__ == "__main__":
    main()
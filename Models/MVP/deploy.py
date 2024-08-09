from pathlib import Path
from google.cloud import bigquery
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import streamlit as st
import pandas as pd
import math
import os
import joblib

from Models.MVP.model import Recommender

st.set_page_config(

)

recommender = Recommender()
data = recommender.data

categorias = data['category'].str.split(', ').explode().unique()
categorias = list(categorias.tolist())

categorias_set = set()

for categoria in categorias:
    # Limpiar los caracteres no deseados
    categoria = categoria.replace("'", "").replace("[", "").replace("]", "")
    
    # Agregar la categoría limpia al conjunto
    categorias_set.add(categoria)

categoria_deseada = st.selectbox(
    'Selecionar Categoría', categorias_set)

estado_deseado = st.selectbox('Selecionar Estado', data['state'].unique())

ciudad_deseado = st.selectbox(
    'Selecionar Ciudad', data[data["state"] == estado_deseado]["city"].unique())

if st.button('Recomendar Restaurantes'):
    if "restaurant" in categoria_deseada:
        categoria_deseada = categoria_deseada.replace("restaurant", "")
    
    st.write(recommender.recomendar_restaurantes(
        categoria_deseada, estado_deseado, ciudad_deseado))

from pathlib import Path
from google.cloud import bigquery
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from PIL import Image
import streamlit as st
import pandas as pd
import math
import os
import joblib

from modelo_restaurant import Recommender

from modelo_city import recommend_city

# Titulo de la pagina
st.title("Sistemas de Recomendaciónes")

col1, col2 = st.columns((1, 2))

try:
    img = Image.open('images/logo.png') 
    col1.image(img, width=250)

except Exception as e:
    st.error(f"Error al cargar la imagen: {e}")

# Selección del modelo de recomendación
model_choice = col2.radio(
    "Selecciona el modelo de recomendación:",
    ("Recomendacion de Restaurantes", "Recomendacion de Ciudad")
)

# Lógica para cada modelo
if model_choice == "Recomendacion de Restaurantes":
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

    categoria_deseada = col2.selectbox(
        'Selecionar Categoría', categorias_set)

    estado_deseado = col2.selectbox('Selecionar Estado', data['state'].unique())

    ciudad_deseado = col2.selectbox(
        'Selecionar Ciudad', data[data["state"] == estado_deseado]["city"].unique())

    if col2.button('Recomendar Restaurantes'):
        if "restaurant" in categoria_deseada:
            categoria_deseada = categoria_deseada.replace("restaurant", "")
        
        col2.write("Recomendaciones Encontradas:")
        col2.dataframe(recommender.recomendar_restaurantes(
            categoria_deseada, estado_deseado, ciudad_deseado), hide_index=True)

    
    
elif model_choice == "Recomendacion de Ciudad":
    
    data = pd.read_json('data/dataML.json', lines=True)

    states = data['state'].unique()

    state = col2.selectbox('Selecionar Estado', states)

    categories = data[data['state'] == state]['category'].explode().unique().tolist()

    category = col2.selectbox('Selecionar Categoria', categories)

    if col2.button('Recomendar'):
        try:
            col2.write(recommend_city(state, category))
        except Exception as e:
            col2.write('No hay suficientes datos de esta categoria en el estado elegido')
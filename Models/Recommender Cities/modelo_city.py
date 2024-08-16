import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

data = pd.read_json('data/dataML.json', lines=True)

def filter_data(state, category):    
    return data[(data['state']  == state) &(data["category"].apply(lambda x: category in x))]

# Calcular competencia (cantidad de restaurantes en la misma categoría y ciudad)
def calculate_competition(data):
    competition = data.groupby('city').size().reset_index(name='competition')
    return data.merge(competition, on='city')

# Crear un proxy de éxito (puede ser popularidad o una combinación)
def calculate_success(data):
    data['success'] = data['rating'] * data['review_count']
    return data

# Pipeline de preprocesamiento y creación de características
def preprocess_data(state, category):
    filtered_data = filter_data(state, category)
    filtered_data = calculate_competition(filtered_data)
    filtered_data = calculate_success(filtered_data)
    return filtered_data

# Entrenar el modelo
def train_model(state, category):
    processed_data = preprocess_data(state, category)
    X = processed_data[['competition', 'rating', 'review_count']]  # Variables predictoras
    y = processed_data['success']  # Variable objetivo
    
    # Dividir en conjuntos de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    # Entrenar el modelo de Random Forest
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Hacer predicciones
    y_pred = model.predict(X_test)
    
    return model, processed_data

# Hacer la recomendación
def recommend_city(state, category):
    model, processed_data = train_model(state, category)
    predictions = model.predict(processed_data[['competition', 'rating', 'review_count']])
    processed_data['predicted_success'] = predictions
    best_city = processed_data.loc[processed_data['predicted_success'].idxmax()]['city']
    return f'La mejor ciudad para abrir un restaurant de {category} en {state} es {best_city}'
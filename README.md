# PF-Reviews
<p align="center">
    <img src="/img/banner.png" alt="Banner-image.png"/>
</p>

## Acerca del proyecto:
Como consultora de datos, realizaremos un análisis exhaustivo del mercado estadounidense para un cliente dentro del conglomerado de empresarios gastronómicos y afines. Nuestro enfoque principal es realizar un análisis exhaustivo de restaurantes, utilizando datos de las plataformas Google Maps y Yelp que nos proporcionará información valiosa para los inversores, ayudándoles en la toma de decisiones estratégicas.

## Roles
- **Data Engineer**: Gustavo Pardo.
- **Data Scientist**: Yair Juarez, Jaime Gold.
- **Data Analyst**: Eliana Larregola, Rocio Alaniz.

## Nuestra misión:
Realizar un análisis exhaustivo de las reseñas de restaurantes en Google Maps y Yelp para proporcionar una visión clara de las oportunidades de mercado, determinar ubicaciones óptimas para nuevos establecimientos y desarrollar un sistema de recomendación  que ayude a los usuarios a encontrar restaurantes basados en su ciudad y preferencias.


## Objetivos del proyecto 
1. Analizar los Sentimientos y Tendencias:
    - Evaluar el sentimiento general de los usuarios hacia los restaurantes, identificando tendencias positivas y negativas en las opiniones para predecir los restaurantes que tendrán mayor crecimiento.

2. Identificar Ubicaciones Óptimas:
    - Analizar datos geográficos para determinar las mejores ubicaciones para abrir nuevos locales de restaurantes.

3. Desarrollar un Sistema de Recomendación:
    - Crear un sistema de recomendación que sugiere restaurantes a los usuarios, basado en la ubicación (ciudad) y sus preferencias.

## Alcance del proyecto
**Categoría**: El análisis se centrará exclusivamente en restaurantes. Otras categorías no serán consideradas en el alcance de este proyecto.

**Área**: Luego de una investigación preliminar sobre los estados en Estados Unidos más poblados y con mayor densidad poblacional, el análisis se limitará a California, Texas, Florida, New York, Pennsylvania.

**Fuentes de datos**: Utilizaremos los datos proporcionados por la compañía que son datos de Yelp y Google Maps.

## Metodología de Desarrollo
Para el desarrollo, hemos adoptado la metodología SCRUM como metodología ágil, lo que nos permite iterar rápidamente y mejorar el MVP basándonos en la retroalimentación y los resultados obtenidos. 

## Estructura del proyecto
```
PF-Reviews/
├── data/
│ ├── processed/
│ └── database/
├── notebooks/
│ ├── exploratory/
│ └── modeling/
├── img/
├── requirements.txt
├── README.md
└── .gitignore
```

## Stack Tecnológicas Utilizadas
<p align="center">
    <img src="/img/stack.jpg" alt="stack-image.jpg"/>
</p>

## Flujo de Trabajo / Pipeline
<p align="center">
    <img src="/img/pipeline.jpg" alt="pipeline-image.jpg"/>
</p>

## Instalación
1. Clone el repositorio.
    ```sh
    git clone https://github.com/JaimeGold/PF-Reviews/PF-Reviews.git```

2. Crear el entorno virtual
    - Linux:
    ```sh
    python -m venv venv
    source venv/bin/activate
    ```
    - Windows:
    ```sh
    python -m venv venv
    .\venv\Scripts\activate 
    ```

3. Instalar las dependencias
    ```sh
    pip install -r requirements.txt```

4. Uso
    ```sh
    python -m ipykernel install --user --name=PF-Reviews --display-name="PF-Reviews"
    ````

## Contacto

* 1 
* 2
* 3
* 4
* 5
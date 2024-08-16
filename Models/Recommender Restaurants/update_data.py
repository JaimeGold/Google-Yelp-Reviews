import os
import pandas as pd
from google.cloud import bigquery

def run_query(query) -> pd.DataFrame:
    # Runs a BigQuery query and returns the results as a pandas DataFrame.
    query_job = client.query(query)
    return query_job.to_dataframe()

# Cargamos el archivo de credenciales de nuestro google cloud en una variable de entorno del PC.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "braided-grammar-430922-b4-64d668a5dc36.json"

# Cargamos el proyecto y el cliente.
project_id = "braided-grammar-430922"
client = bigquery.Client()
id_exists = os.path.exists('data/dataML.json')
# Conseguimos los datos de las empresas.
query = "select * from datanexus.google_yelp_business"
data = run_query(query)

data.drop(columns=['name', 'address', 'gmap_id', 'latitude', 'longitude', 'postal_code'], inplace=True)

data['category'] = data['category'].apply(lambda x: [cat.strip().title() for cat in eval(x) if isinstance(x, str)] 
                                          if isinstance(x, str) and x.startswith('[') 
                                          else [cat.strip().title() for cat in x.split(',')])

categorias_no_comida = [
    "Acupuncture", "Active Life", "Aerial Fitness", "Airport Lounges", 
    "Allergy Testing", "Amusement Parks", "Animal Physical Therapy", 
    "Animal Shelters", "Apartments", "Aquarium Services", "Arcades", 
    "Art Classes", "Art Galleries", "Astrologers", "Auto Detailing", 
    "Auto Parts & Supplies", "Automotive", "Baby Gear & Furniture", 
    "Balloon Services", "Banks & Credit Unions", "Barbers", "Bartenders", 
    "Beauty & Spas", "Bed & Breakfast", "Bingo Halls", 
    "Blood & Plasma Donation Centers", "Boat Dealers", "Bookstores", 
    "Bowling", "Brazilian Jiu-jitsu", "Breweries", "Bridal", 
    "Campgrounds", "Car Dealers", "Car Wash", "Carpeting", "Casinos", 
    "Child Care & Day Care", "Churches", "Cinemas", "Civil Engineering", 
    "Climbing", "Coffee & Tea Supplies", "Comic Books", 
    "Community Service/Non-Profit", "Cosmetology Schools", 
    "Counseling & Mental Health", "Country Clubs", "Dance Schools", 
    "Day Camps", "Dentists", "Department Stores", "Dermatologists", 
    "Digital Marketing", "Disc Golf", "Dive Bars", 
    "Divorce & Family Law", "Drugstores", "Dry Cleaning & Laundry", 
    "Education", "Electricians", "Emergency Pet Hospital", 
    "Employment Agencies", "Entertainment Law", "Escape Games", 
    "Eyewear & Opticians", "Farmers Market", "Fashion", 
    "Financial Services", "Firearm Training", "Fitness & Instruction", 
    "Florists", "Football", "Foundation Repair", "Freelance Photographers", 
    "Furniture Stores", "Garage Door Services", "Gardening Centers", 
    "Gas Stations", "General Contractors", "Golf", "Gymnastics", 
    "Hair Removal", "Hair Salons", "Hardware Stores", "Health Markets", 
    "Hobby Shops", "Home & Garden", "Home Cleaning", "Home Health Care", 
    "Horse Boarding", "Horse Racing", "Hospitals", "Hotels & Travel", 
    "Insurance", "Interior Design", "Internet Service Providers", 
    "It Services & Computer Repair", "Kids Activities", "Libraries", 
    "Marketing", "Martial Arts", "Massage Therapy", "Medical Spas", 
    "Men's Clothing", "Mobile Phone Repair", "Museums", 
    "Music Production Services", "Musical Instruments & Teachers", 
    "Neurologist", "Nightlife", "Noodles", "Office Cleaning", 
    "Oil Change Stations", "Optometrists", "Oral Surgeons", 
    "Organic Stores", "Orthopedists", "Outdoor Furniture Stores", 
    "Paint & Sip", "Painting", "Parks", "Party Bus Rentals", 
    "Party Supplies", "Patisserie/Cake Shop", "Pawn Shops", 
    "Pediatric Dentists", "Pet Boarding", "Pet Groomers", 
    "Pet Services", "Pet Sitting", "Pet Stores", 
    "Photography Stores & Services", "Pilates", "Plumbers", 
    "Podiatrists", "Professional Services", "Property Management", 
    "Public Services & Government", "Real Estate", 
    "Recording & Rehearsal Studios", "Rehearsal Spaces", 
    "Reptile Shops", "Resorts", "Restaurants", "RV Repair", 
    "Sandwiches", "Saunas", "Security Systems", "Self Storage", 
    "Sewing & Alterations", "Shipping Centers", "Shoe Repair", 
    "Shopping", "Skate Shops", "Skydiving", "Social Clubs", 
    "Solar Installation", "Specialty Schools", "Sporting Goods", 
    "Sports Clubs", "Sports Wear", "Stadiums & Arenas", "Steakhouses", 
    "Structural Engineers", "Surfing", "Swimming Lessons/Schools", 
    "Tattoo", "Tax Services", "Tea Rooms", "Team Building Activities", 
    "Tennis", "Towing", "Transmission Repair", "Travel Agents", 
    "Tree Services", "Universities", "Used, Vintage & Consignment", 
    "Veterinarians", "Video/Film Production", "Vinyl Records", 
    "Virtual Reality Centers", "Weight Loss Centers", 
    "Window Washing", "Wine Tasting Classes", "Yoga"
]

data['category'] = data['category'].apply(lambda x: [cat for cat in x if cat not in categorias_no_comida])

# Guardamos los datos verificando que no exista el archivos
data.to_json('data/dataML.json', orient='records', lines=True)
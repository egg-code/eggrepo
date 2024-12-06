import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe.to_dict(orient='records')

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe.to_dict(orient='records')

def extract_from_xml(file_to_process):
    extracted_xml = []
    tree = ET.parse(file_to_process)
    root = tree.getroot()   
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = float(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        extracted_xml.append({
            "car_model":car_model,
            "year_of_manufacture":year_of_manufacture,
            "price":price,
            "fuel":fuel})
        return extracted_xml

#Extract

def extract():
    
    all_data = []

    for csvfile in glob.glob("*.csv"):
        all_data.extend(extract_from_csv(csvfile))

    for jsonfile in glob.glob("*.json"):
        all_data.extend(extract_from_json(jsonfile))

    for xmlfile in glob.glob("*.xml"):
        all_data.extend(extract_from_xml(xmlfile))

    extracted_dataframe = pd.DataFrame(all_data)
    return extracted_dataframe

#Transform
def transform(data):
    data["price"] = round(data.price, 3) # show as 3 decimal point
    return data

#logging and loading
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = "%Y-%m-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ":" + message + "\n")

#Test
log_progress("ETL Job Start")

log_progress("Extract phase Start")
extracted_data = extract()
log_progress("Extract phase End")

log_progress("Transform phase Start")
transformed_data = transform(extracted_data)
log_progress("Transform phase End")

print(transformed_data)

log_progress("Load phase Start")
load_data(target_file, transformed_data)
log_progress("Load phase End")

log_progress("ETL Job Done")
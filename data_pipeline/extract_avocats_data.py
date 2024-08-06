import requests
import pandas as pd
from io import StringIO

URL = "https://www.data.gouv.fr/fr/datasets/r/05f4ca76-0d72-4ecd-9f5c-20a12965e348"

def fetch_avocats_data():
    response = requests.get(URL)
    data = pd.read_csv(StringIO(response.text), sep=';')
    return data.to_dict(orient='records')
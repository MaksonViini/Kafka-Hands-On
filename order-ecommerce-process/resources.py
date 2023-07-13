import requests

def search_items():

    url = "https://shoes-collections.p.rapidapi.com/shoes"

    headers = {
        "X-RapidAPI-Key": "52bfd11dbdmsh0c46cfdc376a6b6p11df9ejsn3f42561d6b2e",
        "X-RapidAPI-Host": "shoes-collections.p.rapidapi.com"
    }

    return requests.get(url, headers=headers).json()
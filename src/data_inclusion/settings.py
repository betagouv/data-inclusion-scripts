import os

# Config for the data.inclusion backend
DI_API_URL = os.environ.get("DI_API_URL", None)
DI_API_TOKEN = os.environ.get("DI_API_TOKEN", None)

# Config for the geocoding backend
BAN_API_URL = os.environ.get("BAN_API_URL", "https://api-adresse.data.gouv.fr/")

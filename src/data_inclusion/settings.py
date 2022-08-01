import os

# Config for the data.inclusion backend
DI_API_URL = os.environ.get("DI_API_URL", None)
DI_API_TOKEN = os.environ.get("DI_API_TOKEN", None)

# Config for the geocoding backend
BAN_API_URL = os.environ.get("BAN_API_URL", "https://api-adresse.data.gouv.fr/")

# Config for the itou source type
ITOU_API_TOKEN = os.environ.get("ITOU_API_TOKEN", None)

# Config for siretization
SIRENE_DATABASE_URL = os.environ.get("SIRENE_DATABASE_URL", None)

# Config for the soliguide source type
SOLIGUIDE_API_TOKEN = os.environ.get("SOLIGUIDE_API_TOKEN", None)
SOLIGUIDE_API_USER_AGENT = os.environ.get("SOLIGUIDE_API_USER_AGENT", None)

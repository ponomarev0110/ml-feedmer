import os

class Constants:
    MODEL_PATH = os.path.join(os.curdir, "resources", "model")
    DEFAULT_LON = os.environ.get('DEFAULT_LON', "53.208655")
    DEFAULT_LAT = os.environ.get('DEFAULT_LAT', "56.853087")
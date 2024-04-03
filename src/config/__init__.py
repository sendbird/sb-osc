from config.config import Config
from config.secret import Secret
from config.env import Env

config = Config()  # override by setting env CONFIG_FILE
secret = Secret()  # override by setting env SECRET_FILE
env = Env()  # override with environment variables

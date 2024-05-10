from config.config import Config
from config.secret import Secret
from config.env import Env

config: Config = Config()  # override by setting env CONFIG_FILE
secret: Secret = Secret()  # override by setting env SECRET_FILE
env: Env = Env()  # override with environment variables

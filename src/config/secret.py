import json
import os

from config.env import Env
from utils import from_string


class Secret:
    """
    Loads secrets from secret file mounted by ExternalSecrets as attributes.
    Default values are used for local environment.
    """
    USERNAME: str = 'root'
    PASSWORD: str = ''
    PORT: int = 3306
    REDIS_HOST: str = ''
    REDIS_PASSWORD: str = ''
    SLACK_CHANNEL: str = ''
    SLACK_TOKEN: str = ''

    def __init__(self, **secrets):
        env = Env()
        if os.path.exists(env.SECRET_FILE):
            with open(env.SECRET_FILE, 'r') as f:
                secrets = json.load(f)
            if secrets:
                for key, value in secrets.items():
                    setattr(self, key.upper(), from_string(value))

        if secrets:
            for key, value in secrets.items():
                setattr(self, key.upper(), from_string(value))

    def __repr__(self):
        repr_dict = self.__annotations__.copy()
        for key in repr_dict:
            repr_dict[key] = '********'
        return str(repr_dict)

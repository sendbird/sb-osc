import os

from utils import from_string


class Env:
    """
    Defines the environment variables used in SB-OSC.
    List of environment variables are defined in the class annotations.
    Default values set in the annotations are used if the environment variable is not set.
    """
    AWS_REGION: str = 'ap-northeast-2'
    POD_NAME: str = 'local'  # POD_NAME = 'local' will determine whether it's running in a local environment or not.
    CONFIG_FILE: str = '/opt/sb-osc/config.yaml'
    SECRET_FILE: str = '/opt/sb-osc/secret.json'

    def __init__(self, **envs):
        """
        Sets the environment variables defined in the class annotations.
        If environment variables are not set, default values are used.
        :param envs: Environment variables to override.
        """
        for env in self.__annotations__:
            default_value = getattr(self, env) if hasattr(self, env) else None
            override_value = envs.get(env)
            if override_value is None:
                # convert string to its original type
                setattr(self, env, from_string(os.getenv(env, str(default_value))))
            else:
                setattr(self, env, override_value)

    def __repr__(self):
        return str(self.__dict__)

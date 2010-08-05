import os


def env_args(var_name, defaults=None, separator=':'):
    """
    Split the given environment variable into an argument tuple.
    
    Empty arguments are replaced with None. Example: 'a::c' -> ('a', None, 'c')
    If the given variable is not set, defaults are returned instead.
    """
    try:
        env_args = os.environ[var_name].split(separator)
    except KeyError:
        env_args = defaults
    else:
        env_args = tuple((arg if arg else None) for arg in env_args)
    
    return env_args

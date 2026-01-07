"""
Utils for io
"""

import os

import yaml

import booktocards


def get_lib_path() -> str:
    """Path to current library"""
    return os.path.join(
        os.path.dirname(booktocards.__file__),
        "..",
    )


def get_conf_path() -> str:
    """Path to conf folder"""
    return os.path.join(
        get_lib_path(),
        "conf",
    )


def get_data_path() -> str:
    """Path to data folder"""
    return os.path.join(
        get_lib_path(),
        "data",
    )


def get_data_sources_path() -> str:
    """Path to sources in the data/in folder"""
    return os.path.join(
        get_data_path(),
        "in",
        "sources",
    )


def _get_dict_folder_path() -> str:
    """Path to folder containing dictionaries"""
    return os.path.join(
        get_data_path(),
        "in",
        "dictionaries",
    )


def get_path_to_dict(dict_name: str) -> str:
    """Path to dictionary"""
    path = os.path.join(_get_dict_folder_path(), dict_name)
    if not os.path.isdir(path):
        raise ValueError(f"{dict_name=} is not a folder in {path}")
    return path


def get_secrets() -> dict:
    """Get conf/secrets.yaml"""
    filepath = os.path.join(get_conf_path(), "secrets.yaml")
    with open(filepath, "r") as f:
        secrets = yaml.safe_load(stream=f)
    return secrets


def get_conf(filename) -> dict:
    """Get conf/`filename`"""
    filepath = os.path.join(get_conf_path(), filename)
    with open(filepath, "r") as f:
        conf = yaml.safe_load(stream=f)
    return conf

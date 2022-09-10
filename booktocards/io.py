"""
Utils for io
"""
import booktocards
import os


def get_lib_path():
    """Path to current library"""
    return os.path.join(
        os.path.dirname(booktocards.__file__),
        "..",
    )


def get_data_path():
    """Path to data folder"""
    return os.path.join(
        get_lib_path(),
        "data",
    )

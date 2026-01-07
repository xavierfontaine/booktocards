import json
import logging
import os
import pickle

import yaml
from yaml import CDumper, CLoader

from booktocards import io
from booktocards.annotations import Definition, DictForm, Reading

# ======
# Logger
# ======
logger = logging.getLogger(__name__)


# =========
# Constants
# =========
_RAW_DICT_DIRNAME = "dictionaries"
_RAW_SENSEIDO_DIRNAME = "三省堂　スーパー大辞林"
_PREPARED_DICT_DIRNAME = "dictionaries"
_PREPARED_SANSEIDO_FILENAME = "sanseido_dict.json"


# ======
# Common
# ======
class NoProcessedDictFound(Exception):
    """Raise when file cannot be found"""


# ========
# Sanseido
# ========
raw_sanseido_filepath = os.path.join(
    io.get_data_path(),
    "in",
    _RAW_DICT_DIRNAME,
    _RAW_SENSEIDO_DIRNAME,
)
_prepared_sanseido_filepath = os.path.join(
    io.get_data_path(),
    "out",
    _PREPARED_DICT_DIRNAME,
    _PREPARED_SANSEIDO_FILENAME,
)


class ManipulateSanseido:
    """Manipulate the 三省堂　スーパー大辞林 dictionary

    The useful method is `self.from_raw_files`, which is th faster way of
    getting a prepared dictionary.

    For saving and loading, json will ensure the less compatibility errors,
    while pickle will be the fastest.

    Attributes:
        sanseido_dict (dict[DictForm, dict[Reading, list[Definition]]])

    Notes:
        - Picke might prove to be a faster (de)serialization method
        - Regarding why pyyaml is so slow: https://stackoverflow.com/a/27744056
    """

    def __init__(self) -> None:
        self.sanseido_dict: dict[DictForm, dict[Reading, list[Definition]]]
        try:
            self._load()
        except NoProcessedDictFound:
            logger.info("-- No processed sanseido found. Processing it.")
            self._from_raw_files()
            self._save()

    def _from_raw_files(self, dirpath: str = raw_sanseido_filepath) -> None:
        """Load sanseido from raw sanseido files"""
        output: dict[DictForm, dict[Reading, list[Definition]]] = dict()
        n_entries = 0  # Counting entries for final check
        for file_id in range(1, 34):
            filename = f"term_bank_{file_id}.json"
            filepath = os.path.join(dirpath, filename)
            with open(file=filepath, mode="r") as f:
                file_entries = json.load(fp=f)
            # Track size
            n_entries += len(file_entries)
            # Transform to usable json
            for entry in file_entries:
                lemma = entry[0]
                reading = entry[1]
                definition = entry[5][0]
                if lemma in output:
                    if reading in output[lemma]:
                        output[lemma][reading].append(definition)
                    else:
                        output[lemma][reading] = [definition]
                else:
                    output[lemma] = dict()
                    output[lemma][reading] = [definition]
            # Check we extracted everything
            assert n_entries == sum(
                len(reading_v)  # type: ignore[misc]
                for lemma_k, lemma_v in output.items()
                for reading_k, reading_v in lemma_v.items()
            )
            # Assign to self
            self.sanseido_dict = output

    def _save(self, filepath: str = _prepared_sanseido_filepath):
        """Save to json"""
        if len(self.sanseido_dict) == 0:
            raise ValueError(
                "The dictionary is empty. Populate it through"
                " `self.from_raw_files` first."
            )
        with open(filepath, "w") as f:
            json.dump(
                obj=self.sanseido_dict,
                fp=f,
            )

    def _load(self, filepath: str = _prepared_sanseido_filepath):
        """Load from json."""
        if not os.path.isfile(filepath):
            raise NoProcessedDictFound(f"-- No file at {filepath}")
        with open(filepath, "r") as f:
            self.sanseido_dict = json.load(fp=f)

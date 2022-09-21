from typing import Literal
import tqdm
import pathlib
import os
import pandas as pd
import logging
from functools import reduce
from datetime import datetime
from jamdict import Jamdict, jmdict

from booktocards import sudachi as jp_sudachi
from booktocards import spacy_utils as jp_spacy
from booktocards import jamdict_utils as jp_jamdict
from booktocards import datacl as jp_dataclasses
from booktocards import iterables, io
from booktocards import parser
from booktocards.datacl import TokenInfo
from booktocards.annotations import Token, Count, SentenceId


# ======
# Logger
# ======
logger = logging.getLogger(__name__)


# =========
# Constants
# =========
_KB_OUT_DIRPATH = "kb"
_OUT_JSON_EXTENSIONS = ".json"
# Data model for df in KnowledgeBase
_DATA_MODEL = {  # table name: [column names]
    "extracted_voc": [
        "token",
        "count",
        "seq_ids",
        "source_name",
        "is_added_to_anki",
    ],
    "extracted_seqs": [
        "sequence_id",
        "sequence",
        "tokens",
        "source_name",
    ],
    "known_voc": ["token", "is_known"],
    "known_kanji": ["kanji", "is_known"],
    "suspended_voc": ["token", "is_suspended_for_source", "source"],
    "suspended_kanji": ["kanji", "is_suspended_for_source", "source"],
}


# ====
# Core
# ====
class NoKBError(Exception):
    """Raise when file cannot be found"""

    pass


class KnowledgeBase:
    """Knowledge base for vocabulary and kanji

    All data are stored in self following the table and column names in
    `_DATA_MODEL`.
    """

    def __init__(self):
        # Path to kb
        self._kb_dirpath = os.path.join(
            io.get_data_path(),
            "out",
            _KB_OUT_DIRPATH,
        )
        # Try load all data, and if impossible, initialize
        self.extracted_voc: pd.DataFrame
        self.extracted_seqs: pd.DataFrame
        self.known_voc: pd.DataFrame
        self.known_kanji: pd.DataFrame
        for df_name in _DATA_MODEL.keys():
            try:
                self._load_df(df_name=df_name)
            except NoKBError:
                logger.info(
                    f"-- {df_name=} did not exist in kb. Initilazing it."
                )
                self.__dict__[df_name] = pd.DataFrame(
                    columns=_DATA_MODEL[df_name]
                )
                self._save_df(df_name=df_name)

    def _load_df(self, df_name: str) -> None:
        """Read pd.DataFrame and attach it to self"""
        filepath = os.path.join(
            self._kb_dirpath,
            df_name + _OUT_JSON_EXTENSIONS,
        )
        if not os.path.isfile(filepath):
            raise NoKBError(f"No file at {filepath}")
        with open(filepath, "r") as f:
            self.__dict__[df_name] = pd.read_json(
                path_or_buf=f, orient="table"
            )

    def _save_df(self, df_name: str) -> None:
        """Write pd.DataFrame from self to json"""
        filepath = os.path.join(
            self._kb_dirpath,
            df_name + _OUT_JSON_EXTENSIONS,
        )
        with open(filepath, "w") as f:
            self.__dict__[df_name].to_json(
                path_or_buf=f, orient="table", index=False
            )

    def _save_kb(self) -> None:
        # TODO: docstr
        for df_name in _DATA_MODEL.keys():
            self._save_df(df_name=df_name)

    def add_doc(self, doc: str, doc_name: str) -> None:
        """Parse a document and add it's voc and sentences to kb

        `doc_name` is not path-related, and rather used as reference in the kb."""
        if (
            doc_name in self.extracted_voc["source_name"].values
            or doc_name in self.extracted_seqs["source_name"].values
        ):
            raise ValueError(
                f"Trying to add {doc_name=} to the kb, but already exists. Use"
                " self.remove_doc if needed."
            )
        # Get token and sentence info
        logger.info(f"-- parsing {doc_name=}")
        parsed_doc = parser.ParseDocument(doc=doc)
        token_count_sentid = parsed_doc.tokens
        sentid_sent_toks = parsed_doc.sentences
        # To self - extracted voc
        self.extracted_voc = pd.concat(
            [
                self.extracted_voc,
                pd.DataFrame(
                    {
                        "token": token_count_sentid.keys(),
                        "count": [v[0] for v in token_count_sentid.values()],
                        "seq_ids": [v[1] for v in token_count_sentid.values()],
                        "source_name": [
                            doc_name for i in range(len(token_count_sentid))
                        ],
                        "is_added_to_anki": [
                            False for i in range(len(token_count_sentid))
                        ],
                    }
                ),
            ]
        )
        # To self - extracted seqs
        self.extracted_seqs = pd.concat(
            [
                self.extracted_seqs,
                pd.DataFrame(
                    {
                        "sequence_id": sentid_sent_toks.keys(),
                        "sequence": [v[0] for v in sentid_sent_toks.values()],
                        "tokens": [v[1] for v in sentid_sent_toks.values()],
                        "source_name": [
                            doc_name for i in range(len(sentid_sent_toks))
                        ],
                    }
                ),
            ]
        )
        # Save in kb
        self._save_kb()
        logger.info(f"-- Added {doc_name=} to kb.")

    def remove_doc(self, doc_name: str):
        """Remove doc from kb"""
        # Remove frm extracted_seqs
        rows_to_remove = self.extracted_seqs["source_name"] == doc_name
        self.extracted_seqs = self.extracted_seqs[~rows_to_remove]
        # Remove frm extracted_voc
        rows_to_remove = self.extracted_voc["source_name"] == doc_name
        self.extracted_voc = self.extracted_voc[~rows_to_remove]
        logger.info(f"-- Dropped {doc_name=} from kb.")
        self._save_kb()

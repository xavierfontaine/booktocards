import copy
import datetime
import logging
import os
from dataclasses import dataclass
from typing import Optional

import deepl
import pandas as pd

from booktocards import io, jamdict_utils, parser
from booktocards.annotations import ColName, Count, Sentence, SentenceId, Token, Values
from booktocards.datacl import (
    KanjiCard,
    TokenInfo,
    VocabCard,
    kanji_info_to_kanji_card,
    token_info_to_voc_cards,
)
from booktocards.jj_dicts import ManipulateSanseido
from booktocards.tatoeba import ManipulateTatoeba
from booktocards.text import get_unique_kanjis, is_only_ascii_alphanum

# ======
# Logger
# ======
logger = logging.getLogger(__name__)


# =========
# Constants
# =========
_KB_OUT_DIRNAME = "kb"
_OUT_PICKLE_EXTENSION = ".pickle"


# =========
# Enums
# =========
@dataclass
class TableName:
    TOKENS = "tokens_df"
    KANJIS = "kanjis_df"
    SEQS = "seqs_df"
    DOCS = "docs_df"


@dataclass
class ColumnName:
    TOKEN = "token"
    KANJI = "kanji"
    SEQ = "seq"
    CARD_TABLE = "card"
    COUNT = "count"
    SEQ_ID = "seq_id"
    SEQS_IDS = "seqs_ids"
    SOURCE_NAME = "source_name"
    IS_KNOWN = "is_known"
    IS_ADDED_TO_ANKI = "is_added_to_anki"
    IS_SUSPENDED_FOR_SOURCE = "is_suspended_for_source"
    TO_BE_STUDIED_FROM = "to_be_studied_from"
    ASSOCIATED_TOKS_FROM_SOURCE = "associated_toks_from_source"
    HIDE_IN_ADD_FULL_DOC_APP = "hide_in_add_full_doc_app"
    PRIORITY = "priority"  # With 0 (low), 1 (normal), 2 (high)


DATA_MODEL = {  # table name: {column name: pandas dtype}
    # 'string' instead of 'str' prevents dtype mixing.
    # 'boolean' instead of 'bool' allows for NA values.
    TableName.TOKENS: {
        ColumnName.TOKEN: "string",
        ColumnName.COUNT: "int",
        ColumnName.SEQS_IDS: "object",  # list of str
        ColumnName.SOURCE_NAME: "string",
        ColumnName.IS_KNOWN: "boolean",  # NA if no deecided yet
        ColumnName.IS_ADDED_TO_ANKI: "bool",
        ColumnName.IS_SUSPENDED_FOR_SOURCE: "bool",
        ColumnName.TO_BE_STUDIED_FROM: "object",
        ColumnName.PRIORITY: "int8",
    },
    TableName.KANJIS: {
        ColumnName.KANJI: "string",
        ColumnName.ASSOCIATED_TOKS_FROM_SOURCE: "object",  # list of str
        ColumnName.IS_KNOWN: "boolean",  # NA if no deecided yet
        ColumnName.IS_ADDED_TO_ANKI: "bool",
        ColumnName.IS_SUSPENDED_FOR_SOURCE: "bool",
        ColumnName.SOURCE_NAME: "string",
    },
    TableName.SEQS: {
        ColumnName.SEQ: "string",
        ColumnName.SEQ_ID: "int",
        ColumnName.ASSOCIATED_TOKS_FROM_SOURCE: "object",  # list of str
        ColumnName.SOURCE_NAME: "string",
    },
    TableName.DOCS: {
        ColumnName.SOURCE_NAME: "string",
        ColumnName.HIDE_IN_ADD_FULL_DOC_APP: "bool",
    },
}


# ====
# Core
# ====
# Custom exception
class NoKBError(Exception):
    """Raise when file cannot be found"""


class NotInJamdictError(Exception):
    """Raise when token is not in jamdict"""


class TokenAlreadyExistsForSourceInKbError(Exception):
    """Raise when trying to add an item that already exists in the kb"""


class SourceAlreadyExistsInKbError(Exception):
    """Raise when trying to add a doc that already exists in the kb"""


class NonExistentDocError(Exception):
    """Raise when trying to access a doc that does not exist in the kb"""


class TokenOrKanjiOrSeqAlreadyExistsForSourceInKbError(Exception):
    """Raise when trying to add an item that already exists in the kb"""


# Path to kb
_kb_dirpath = os.path.join(
    io.get_data_path(),
    "out",
    _KB_OUT_DIRNAME,
)


# KB class
class KnowledgeBase:
    """Knowledge base for vocabulary and kanji

    At runtime, data are stored in self. At instantiating, they are loaded from
    pickle.

    Attributes
        One pd.DataFrame per key in `_DATA_MODEL`. The columns of dataframe p
        are the values in `_DATA_MODEL`[p]
    """

    def __init__(self, kb_dirpath: str = _kb_dirpath):
        # Attach arguments to self
        self.kb_dirpath = kb_dirpath
        # Get now's timestamp for naming folders
        self.now = str(datetime.datetime.now())
        # Try load all data
        try:
            self._load_kb()
        # If impossible, initialize the db and save it
        except NoKBError:
            logger.info("-- No existing kb. Initializing it.")
            # Create table
            for df_name in DATA_MODEL.keys():
                self.__dict__[df_name] = pd.DataFrame(
                    columns=list(DATA_MODEL[df_name].keys())
                ).astype(DATA_MODEL[df_name])
            logger.info("-- Knowledge base initialized. Save it with `save_kb`.")

    def __getitem__(self, arg) -> pd.DataFrame:
        """Get tables through square brakets"""
        if arg not in DATA_MODEL.keys():
            raise KeyError(f"{arg} is not one of {DATA_MODEL.keys()}")
        return self.__dict__[arg]

    def _load_df(self, df_name: str) -> None:
        """Read pd.DataFrame and attach it to self"""
        filepath = os.path.join(
            self.kb_dirpath,
            df_name + _OUT_PICKLE_EXTENSION,
        )
        if not os.path.isfile(filepath):
            raise NoKBError(f"No file at {filepath}")
        with open(filepath, "rb") as f:
            self.__dict__[df_name] = pd.read_pickle(
                filepath_or_buffer=f,
            )

    def _save_df(self, df_name: str, is_backup: bool) -> None:
        """Write pd.DataFrame from self to pickle

        If `is_backup`, save into a subrepo named after self's instantiation
        time's timestamp.
        """
        if is_backup:
            dirpath = os.path.join(
                self.kb_dirpath,
                self.now,
            )
        else:
            dirpath = self.kb_dirpath
        if not os.path.isdir(dirpath):
            os.mkdir(dirpath)
        filepath = os.path.join(
            dirpath,
            df_name + _OUT_PICKLE_EXTENSION,
        )
        with open(filepath, "wb") as f:
            self.__dict__[df_name].to_pickle(path=f)

    def _load_kb(self) -> None:
        """Load the kb from pickle"""
        for df_name in DATA_MODEL.keys():
            self._load_df(df_name=df_name)

    def save_kb(self, make_backup: bool = True) -> None:
        """Save the kb

        Under the hood, the kb i saved into several pickle files (1 per table).
        These pickle will be loaded upon re-instantiation of KnowledgeBase.

        If `make_backup`, will make another save in a backup directory.
        """
        for df_name in DATA_MODEL.keys():
            self._save_df(df_name=df_name, is_backup=False)
            if make_backup:
                self._save_df(df_name=df_name, is_backup=True)

    def create_source_entry(
        self,
        source_name: str,
        hide_in_add_full_doc_app: bool = False,
    ) -> None:
        """Create a source entry in the docs table

        Args:
            source_name (str): name to be used as reference in kb. Not a path or
                filename.
            hide_in_add_full_doc_app (bool): hide in add full doc app?

        Raises:
            ValueError: if `source_name` already exists in the kb.
        """
        if source_name in self.__dict__[TableName.DOCS][ColumnName.SOURCE_NAME].values:
            raise SourceAlreadyExistsInKbError(
                f"Trying to add {source_name=} to the kb docs, but already exists."
            )
        self._add_items(
            entry_to_add={
                ColumnName.SOURCE_NAME: [source_name],
                ColumnName.HIDE_IN_ADD_FULL_DOC_APP: [hide_in_add_full_doc_app],
            },
            table_name=TableName.DOCS,
        )

    def add_doc_from_full_text(
        self,
        doc: str,
        doc_name: str,
        drop_ascii_alphanum_toks: bool,
        sep_tok: Optional[str] = None,
    ) -> None:
        """Parse a document and add it's voc and sentences to kb.

        Set priority to 1 (normal) for all tokens.

        Args:
            doc (str): doc
            doc_name (str): name to be used as reference in kb. Not a path or
                filename.
            drop_ascii_alphanum_toks (bool): discard tokens that are only ascii
                alphanum?
            sep_tok (Optional[str]): special token for sentence separation

        Raises:
            TokenOrKanjiOrSeqAlreadyExistsForSourceInKbError: if some tokens,
                kanjis or sequences from `doc_name` already exist in the kb.
        """
        if doc_name not in self.__dict__[TableName.DOCS][ColumnName.SOURCE_NAME].values:
            raise NonExistentDocError(
                f"{doc_name=} not found in kb docs. Create it first using"
                " `create_source_entry`."
            )

        # NOTE: current code does not allow pre-existing entries (at the very least,
        # sequence ids would be wrong). Hence the check below.
        if (
            doc_name in self.__dict__[TableName.TOKENS][ColumnName.SOURCE_NAME].values
            or doc_name
            in self.__dict__[TableName.KANJIS][ColumnName.SOURCE_NAME].values
            or doc_name in self.__dict__[TableName.SEQS][ColumnName.SOURCE_NAME].values
        ):
            raise TokenOrKanjiOrSeqAlreadyExistsForSourceInKbError(
                f"Trying to add entries for {doc_name=} to the kb, but somes already"
                " exists. Use self.remove_doc if needed."
            )

        # Get token and sentence info
        logger.info(f"-- parsing {doc_name=}")
        parsed_doc = parser.ParseDocument(doc=doc, sep_tok=sep_tok)
        token_count_sentid: dict[Token, tuple[Count, list[SentenceId]]] = (
            parsed_doc.tokens
        )
        sentid_sent_toks = parsed_doc.sentences
        # Drop tokens that are pure alphanum if required
        if drop_ascii_alphanum_toks:
            token_count_sentid = {
                k: v
                for k, v in token_count_sentid.items()
                if not is_only_ascii_alphanum(text=k)
            }
        # Get kanjis
        unique_kanjis = get_unique_kanjis(doc)
        # Get associated tokens
        uniq_kanjis_w_toks = {
            kanji: [tok for tok in token_count_sentid.keys() if kanji in tok]
            for kanji in unique_kanjis
        }
        # To self - extracted voc
        self._add_items(
            entry_to_add={
                ColumnName.TOKEN: list(token_count_sentid.keys()),
                ColumnName.COUNT: [v[0] for v in token_count_sentid.values()],
                ColumnName.SEQS_IDS: [v[1] for v in token_count_sentid.values()],
                ColumnName.SOURCE_NAME: [
                    doc_name for i in range(len(token_count_sentid))
                ],
                ColumnName.IS_KNOWN: [pd.NA for i in range(len(token_count_sentid))],
                ColumnName.IS_ADDED_TO_ANKI: [
                    False for i in range(len(token_count_sentid))
                ],
                ColumnName.IS_SUSPENDED_FOR_SOURCE: [
                    False for i in range(len(token_count_sentid))
                ],
                ColumnName.TO_BE_STUDIED_FROM: [
                    None for i in range(len(token_count_sentid))
                ],
                ColumnName.PRIORITY: [1 for i in range(len(token_count_sentid))],
            },
            table_name=TableName.TOKENS,
            item_colname=ColumnName.TOKEN,
        )
        # To self - kanjis
        self._add_items(
            entry_to_add={
                ColumnName.KANJI: list(uniq_kanjis_w_toks.keys()),
                ColumnName.ASSOCIATED_TOKS_FROM_SOURCE: list(
                    uniq_kanjis_w_toks.values()
                ),
                ColumnName.IS_KNOWN: [pd.NA for i in range(len(uniq_kanjis_w_toks))],
                ColumnName.IS_ADDED_TO_ANKI: [
                    False for i in range(len(uniq_kanjis_w_toks))
                ],
                ColumnName.IS_SUSPENDED_FOR_SOURCE: [
                    False for i in range(len(uniq_kanjis_w_toks))
                ],
                ColumnName.SOURCE_NAME: [
                    doc_name for i in range(len(uniq_kanjis_w_toks))
                ],
            },
            table_name=TableName.KANJIS,
            item_colname=ColumnName.KANJI,
        )
        # To self - extracted sequences
        self._add_items(
            entry_to_add={
                ColumnName.SEQ_ID: list(sentid_sent_toks.keys()),
                ColumnName.SEQ: [v[0] for v in sentid_sent_toks.values()],
                ColumnName.ASSOCIATED_TOKS_FROM_SOURCE: [
                    v[1] for v in sentid_sent_toks.values()
                ],
                ColumnName.SOURCE_NAME: [
                    doc_name for i in range(len(sentid_sent_toks))
                ],
            },
            table_name=TableName.SEQS,
        )
        # Save in kb
        logger.info(f"-- Added {doc_name=} to kb.")

    def add_token_with_sequence_to_doc(
        self,
        token: str,
        sequence: Optional[str],
        doc_name: str,
        sep_tok: Optional[str] = None,
    ) -> None:
        """Add a single token with an associated sequence to the kb

        Forcibly set the count to 1 if sequence is provided, 0 otherwise.

        Forcibly set the priority to 2 (high).

        Args:
            token (str): token
            sequence (Optional[str]): associated sequence
            doc_name (str): name to be used as reference in kb. Not a path or
                filename.
            sep_tok (Optional[str]): special token for sentence separation

        Raises:
            NonExistentDocError: if `doc_name` does not exist in the kb
            NotInJamdictError: if `token` is not found in jamdict
            TokenAlreadyExistsForSourceInKbError: if `token` already exists
                for `doc_name` in the kb.
        """
        # Check that doc exists
        if doc_name not in self.__dict__[TableName.DOCS][ColumnName.SOURCE_NAME].values:
            raise NonExistentDocError(
                f"{doc_name=} not found in kb docs. Create it first using"
                " `create_source_entry`."
            )

        # Check for presence in jamdict
        dict_entries = jamdict_utils.get_dict_entries(
            query=token,
            drop_unfreq_entries=True,
            drop_unfreq_readings=True,
            strict_lookup=True,
        )
        if len(dict_entries) == 0:
            raise NotInJamdictError(f"{token=} not found in jamdict.")

        # Check that token does not already exist for source
        if (
            self.get_items(
                table_name=TableName.TOKENS,
                only_not_added=False,
                only_not_known=False,
                only_not_suspended=False,
                only_no_study_date=False,
                item_value=token,
                item_colname=ColumnName.TOKEN,
                source_name=doc_name,
            ).shape[0]
            > 0
        ):
            raise TokenAlreadyExistsForSourceInKbError(
                f"Trying to add {token=} for {doc_name=} to the kb, but already"
                " exists."
            )

        # Determine the sentence id as the next available id for the source
        sequence_id: int | None = None
        if sequence is not None:
            seqs_df = self.__dict__[TableName.SEQS]
            is_source = seqs_df[ColumnName.SOURCE_NAME] == doc_name
            if any(is_source):
                max_seq_id = seqs_df[is_source][ColumnName.SEQ_ID].max()
                sequence_id = max_seq_id + 1
            else:
                sequence_id = 0
        sequence_id_list = [sequence_id] if sequence_id is not None else []

        # Determine the count for the token
        if sequence is not None:
            count = 1
        else:
            count = 0

        # Construct the token/count/sentid dict
        token_count_sentid: dict[Token, tuple[Count, list[SentenceId]]] = {
            token: (count, sequence_id_list)
        }

        # Construct the sentid/sent/toks dict
        sentid_sent_toks: dict[SentenceId, tuple[Sentence, list[Token]]] = {}
        if sequence is not None:
            assert sequence_id is not None
            sentid_sent_toks[sequence_id] = (sequence, [token])

        # Get kanjis
        unique_kanjis = get_unique_kanjis(token)
        uniq_kanjis_w_toks = {kanji: [token] for kanji in unique_kanjis}

        # Remove unique kanjis that are already in the kb for the source
        uniq_kanjis_w_toks = {
            kanji: toks
            for kanji, toks in uniq_kanjis_w_toks.items()
            if not any(
                (self.__dict__[TableName.KANJIS][ColumnName.KANJI] == kanji)
                & (self.__dict__[TableName.KANJIS][ColumnName.SOURCE_NAME] == doc_name)
            )
        }

        # To self - extracted voc
        self._add_items(
            entry_to_add={
                ColumnName.TOKEN: list(token_count_sentid.keys()),
                ColumnName.COUNT: [v[0] for v in token_count_sentid.values()],
                ColumnName.SEQS_IDS: [v[1] for v in token_count_sentid.values()],
                ColumnName.SOURCE_NAME: [
                    doc_name for i in range(len(token_count_sentid))
                ],
                ColumnName.IS_KNOWN: [pd.NA for i in range(len(token_count_sentid))],
                ColumnName.IS_ADDED_TO_ANKI: [
                    False for i in range(len(token_count_sentid))
                ],
                ColumnName.IS_SUSPENDED_FOR_SOURCE: [
                    False for i in range(len(token_count_sentid))
                ],
                ColumnName.TO_BE_STUDIED_FROM: [
                    None for i in range(len(token_count_sentid))
                ],
                ColumnName.PRIORITY: [2 for i in range(len(token_count_sentid))],
            },
            table_name=TableName.TOKENS,
            item_colname=ColumnName.TOKEN,
        )
        # To self - kanjis
        self._add_items(
            entry_to_add={
                ColumnName.KANJI: list(uniq_kanjis_w_toks.keys()),
                ColumnName.ASSOCIATED_TOKS_FROM_SOURCE: list(
                    uniq_kanjis_w_toks.values()
                ),
                ColumnName.IS_KNOWN: [pd.NA for i in range(len(uniq_kanjis_w_toks))],
                ColumnName.IS_ADDED_TO_ANKI: [
                    False for i in range(len(uniq_kanjis_w_toks))
                ],
                ColumnName.IS_SUSPENDED_FOR_SOURCE: [
                    False for i in range(len(uniq_kanjis_w_toks))
                ],
                ColumnName.SOURCE_NAME: [
                    doc_name for i in range(len(uniq_kanjis_w_toks))
                ],
            },
            table_name=TableName.KANJIS,
            item_colname=ColumnName.KANJI,
        )
        # To self - extracted sequences
        if len(sentid_sent_toks) > 0:
            self._add_items(
                entry_to_add={
                    ColumnName.SEQ_ID: list(sentid_sent_toks.keys()),
                    ColumnName.SEQ: [v[0] for v in sentid_sent_toks.values()],
                    ColumnName.ASSOCIATED_TOKS_FROM_SOURCE: [
                        v[1] for v in sentid_sent_toks.values()
                    ],
                    ColumnName.SOURCE_NAME: [
                        doc_name for i in range(len(sentid_sent_toks))
                    ],
                },
                table_name=TableName.SEQS,
            )
        # Save in kb
        logger.info(f"-- Added entry {token=} for {doc_name=} to kb.")

    def _add_items(
        self,
        entry_to_add: dict[ColName, Values],
        table_name: str,
        item_colname: Optional[ColName] = None,
    ) -> None:
        """Add item to the table.

        Ensure consistency of the known status if `item_colname` is specified.

        Args:
            entry_to_add (dict[ColName, Values]): row to add to the table
            table_name (str): name of the table. One of TableName.TOKENS,
                TableName.KANJIS, TableName.SEQS, TableName.DOCS.
            item_colname (Optional[ColName]): if specified, will check that if
                the new entry shares values of `item_colname` with another
                entry, then it will take the value of ColumnName.IS_KNOWN from
                that other entry. For instance, if we add a token frm a new
                source, but that token already existed in the database and was
                marked as known, then the new entry will also be
                considered known.

        Returns:
            None
        """
        if table_name not in DATA_MODEL.keys():
            raise KeyError(f"{table_name} is not one of {DATA_MODEL.keys()}")
        items_to_add = copy.deepcopy(entry_to_add)
        # Check keys
        if set(items_to_add.keys()) != set(self.__dict__[table_name].columns.to_list()):
            raise KeyError(
                f"Keys in `items_to_add` are {items_to_add.keys()}, but should be"
                f" {DATA_MODEL[table_name]}"
            )
        # Check value length
        columns = list(items_to_add.keys())
        for col in columns[1:]:
            if len(items_to_add[columns[0]]) != len(items_to_add[col]):
                raise ValueError(
                    f"Column {col} in data_dict doesn't have the same length"
                    " as other columns."
                )
        # If seqs_df, ensure the seq_id does not already exist for the source
        if table_name == TableName.SEQS:
            merge_df = pd.merge(
                self.__dict__[TableName.SEQS],
                pd.DataFrame(items_to_add),
                on=[ColumnName.SEQ_ID, ColumnName.SOURCE_NAME],
                how="inner",
            )
            if len(merge_df) > 0:
                raise ValueError(
                    f"Trying to add seqs with {ColumnName.SEQ_ID} that already"
                    " exist for the same source. Conflicting ids are:"
                    f" {merge_df[ColumnName.SEQ_ID].tolist()}"
                )
        # For an added row, if the value for the index exist and associated to
        # a known value/added to Anki/has a set due date, then set know to True
        if item_colname is not None:
            for obs_i, index_value in enumerate(items_to_add[item_colname]):
                # Check conditions separetely
                table = self.__dict__[table_name]
                value_exists = table[item_colname] == index_value
                value_know = table[ColumnName.IS_KNOWN].isin([True])  # Handle NA
                value_added_to_anki = table[ColumnName.IS_ADDED_TO_ANKI]
                if table_name == TableName.TOKENS:
                    value_to_be_studied = table[ColumnName.TO_BE_STUDIED_FROM].apply(
                        lambda x: False if type(x) is not datetime.date else True
                    )
                # Put the conditions together
                if table_name == TableName.TOKENS:
                    value_exists_and_marked = value_exists & (
                        value_know | value_added_to_anki | value_to_be_studied
                    )
                elif table_name == TableName.KANJIS:
                    value_exists_and_marked = value_exists & (
                        value_know | value_added_to_anki
                    )
                else:
                    raise ValueError("Unexpected table name.")
                # If so...
                if any(value_exists_and_marked):
                    logger.debug(
                        f"{index_value} exists in {table_name}[{item_colname}]"
                        " and is marked as known/added or has a 'from' study "
                        "date (if token table). Mark it as"
                        " know in the added items as well."
                    )
                    items_to_add[ColumnName.IS_KNOWN][obs_i] = True
        # Add the values
        self.__dict__[table_name] = pd.concat(
            [
                self.__dict__[table_name],
                pd.DataFrame(items_to_add).astype(DATA_MODEL[table_name]),
            ]
        )

    def set_item_to_known(
        self,
        item_value: str,
        item_colname: ColName,
        table_name: str,
    ) -> None:
        """Set item to known in all sources.

        Args:
            item_value (str): value of the item in column `item_colname`
            item_colname (ColName): column on which we should look for
                `item_value`
            table_name (str): name of the table. One of TableName.TOKENS,
                TableName.KANJIS.

        Returns:
            None:
        """
        if table_name not in [
            TableName.TOKENS,
            TableName.KANJIS,
        ]:
            raise ValueError(
                f"{table_name=} not in {[TableName.TOKENS, TableName.KANJIS]}"
            )
        # Find where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to known in all sources
        self.__dict__[table_name].loc[is_item, ColumnName.IS_KNOWN] = True

    def set_item_to_unknown(
        self,
        item_value: str,
        item_colname: ColName,
        table_name: str,
    ) -> None:
        """Inverse of `set_item_to_known`"""
        if table_name not in [
            TableName.TOKENS,
            TableName.KANJIS,
        ]:
            raise ValueError(
                f"{table_name=} not in {[TableName.TOKENS, TableName.KANJIS]}"
            )
        # Find where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to unknown in all sources
        self.__dict__[table_name].loc[is_item, ColumnName.IS_KNOWN] = False

    def set_item_to_added_to_anki(
        self,
        item_value: str,
        source_name: str,
        item_colname: ColName,
        table_name: str,
    ) -> None:
        """Set item as added to Anki.

        Do not set the item as known.

        Args:
            item_value (str): value of the item we want to set as added
            source_name (str): source of the item we want to set as added
            item_colname (ColName): column in which we will look for
                `item_value`
            table_name (str): name of the table. One of TableName.TOKENS,
                TableName.KANJIS.

        Returns:
            None
        """
        if table_name not in [
            TableName.TOKENS,
            TableName.KANJIS,
        ]:
            raise ValueError(
                f"{table_name=} not in {[TableName.TOKENS, TableName.KANJIS]}"
            )
        # Find where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        is_source = self.__dict__[table_name][ColumnName.SOURCE_NAME] == source_name
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to added to anki in all sources
        self.__dict__[table_name].loc[
            is_item & is_source, ColumnName.IS_ADDED_TO_ANKI
        ] = True

    def set_item_to_suspended_for_source(
        self,
        item_value: str,
        source_name: str,
        item_colname: ColName,
        table_name: str,
    ) -> None:
        """Set item as suspended for a given source

        Args:
            item_value (str): value of the item we want to set as suspended
            source_name (str): source of the item we want to set as suspended
            item_colname (ColName): column in which we will look for
                `item_value`
            table_name (str): name of the table. One of TableName.TOKENS,
                TableName.KANJIS.

        Returns:
            None
        """
        if table_name not in [
            TableName.TOKENS,
            TableName.KANJIS,
        ]:
            raise ValueError(
                f"{table_name=} not in {[TableName.TOKENS, TableName.KANJIS]}"
            )
        # Find where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        is_source = self.__dict__[table_name][ColumnName.SOURCE_NAME] == source_name
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to known in all sources
        self.__dict__[table_name].loc[
            is_item & is_source, ColumnName.IS_SUSPENDED_FOR_SOURCE
        ] = True

    def set_study_from_date_for_token_source(
        self,
        token_value: str,
        source_name: str,
        date: datetime.date,
    ) -> None:
        """Set the 'study from' date for item

        The date is stored in ColumnName.TO_BE_STUDIED_FROM
        """
        # Find where ColumnName.TOKEN is equal to item_value
        is_item = self.__dict__[TableName.TOKENS][ColumnName.TOKEN] == token_value
        is_source = (
            self.__dict__[TableName.TOKENS][ColumnName.SOURCE_NAME] == source_name
        )
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{token_value=} cannot be found in {TableName.TOKENS}[{ColumnName.TOKEN}]"
            )
        # Set data
        self.__dict__[TableName.TOKENS].loc[
            is_item & is_source, ColumnName.TO_BE_STUDIED_FROM
        ] = date

    def get_items(
        self,
        table_name: str,
        only_not_added: bool,
        only_not_known: bool,
        only_not_suspended: bool,
        only_no_study_date: bool,
        item_value: Optional[str] = None,
        item_colname: Optional[ColName] = None,
        source_name: Optional[str] = None,
        max_study_date: Optional[datetime.date] = None,
        priority: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get items corresponding to input conditions.

        Returns a copy of the relevant rows in the table.

        Args:
            table_name (str): table name. One of TableName.TOKENS,
                TableName.KANJIS.
            only_not_added (bool): retrieve only those items with no True in
                ColumnName.IS_ADDED_TO_ANKI.
            only_not_known (bool): retrieve only those items with no True in
                ColumnName.IS_KNOWN (all False or NA.)
            only_not_added (bool): retrieve only those items with no True in
                ColumnName.IS_SUSPENDED_FOR_SOURCE.
            only_not_added (bool): only with no study date specified in
                ColumnName.TO_BE_STUDIED_FROM
            item_value (Optional[str]): value of the item. `item_colname` must
                be set.
            item_colname (Optional[ColName]): name of the column in which to
                look for the item.
            source_name (Optional[str]): name of the source
            max_study_date (Optional[datetime.date]): retrieve items with no
                study date of study date <= max_study_date.
            priority (Optional[int]): if table_name is TableName.TOKENS, filter
                by priority.

        Returns:
            pd.DataFrame
        """
        if table_name not in [
            TableName.TOKENS,
            TableName.KANJIS,
        ]:
            raise ValueError(
                f"{table_name=} not in {[TableName.TOKENS, TableName.KANJIS]}"
            )
        df = self.__dict__[table_name]
        # Sanity
        if only_no_study_date and max_study_date is not None:
            raise ValueError(f"{only_no_study_date=} but {max_study_date=}")
        # Identify rows
        is_items_rows = pd.Series(True, index=df.index)
        if item_value is not None:
            if item_colname is None:
                raise ValueError("`item_colname` must be set when `item_value` is set.")
            is_items_rows = is_items_rows & (df[item_colname] == item_value)
        if source_name is not None:
            is_items_rows = is_items_rows & (df[ColumnName.SOURCE_NAME] == source_name)
        if only_not_added:
            is_items_rows = is_items_rows & ~df[ColumnName.IS_ADDED_TO_ANKI]
        if only_not_known:
            is_items_rows = is_items_rows & ~df[ColumnName.IS_KNOWN].fillna(False)
        if only_not_suspended:
            is_items_rows = is_items_rows & ~df[ColumnName.IS_SUSPENDED_FOR_SOURCE]
        if only_no_study_date:
            # No study date on kanji
            if table_name == TableName.KANJIS:
                pass
            else:
                is_items_rows = is_items_rows & (
                    df[ColumnName.TO_BE_STUDIED_FROM].isnull()
                )
        if max_study_date is not None:
            if table_name == TableName.KANJIS:
                raise ValueError(
                    "`last_study_day` was provided but is not relevant to kanjis"
                )
            is_before_max_and_not_null = df.loc[:, ColumnName.TO_BE_STUDIED_FROM].apply(
                lambda x: False if type(x) is not datetime.date else x <= max_study_date
            )
            is_items_rows = is_items_rows & is_before_max_and_not_null
        if priority is not None:
            if table_name != TableName.TOKENS:
                raise ValueError("`priority` filter is only relevant to tokens table.")
            is_items_rows = is_items_rows & (df[ColumnName.PRIORITY] == priority)
        # Sanity
        assert (
            is_items_rows.isna().sum() == 0
        ), "Internal code error: is_items_rows contains NA values."
        return df[is_items_rows].copy()

    def remove_doc(self, doc_name: str):
        """Remove doc from kb"""
        for table_name in DATA_MODEL.keys():
            rows_to_remove = (
                self.__dict__[table_name][ColumnName.SOURCE_NAME] == doc_name
            )
            self.__dict__[table_name] = self.__dict__[table_name][~rows_to_remove]
            logger.info(f"-- Dropped {doc_name=} from {table_name}")

    def make_voc_cards(
        self,
        token: str,
        source_name: str,
        translate_source_ex: bool,
        max_source_examples: int,
        max_tatoeba_examples: int,
        sanseido_manipulator: ManipulateSanseido,
        tatoeba_db: ManipulateTatoeba,
        deepl_translator: Optional[deepl.Translator] = None,
        ex_linebreak_repl: Optional[str] = None,
    ) -> list[VocabCard]:
        """Make vocabulary card for `token` in `source_name`

        Args:
            token (str): token
            source_name (str): name of the source
            translate_source_ex (bool): add translations for examples extracted
                from source?
            max_source_examples (int): maximum number of examples kept from a
                source
            max_tatoeba_examples (int): maximum number of examples kept from
                tatoeba
            sanseido_manipulator (ManipulateSanseido): ManipulateSanseido
                object
            tatoeba_db (ManipulateTatoeba): ManipulateTatoeba object
            deepl_translator (Optional[deepl.Translator]): deepl.Translator object
            ex_linebreak_repl (Optional[str] = None): str to replace linebreaks
                in examples

        Returns:
            list[VocabCard]
        """
        # Get the associated entry
        token_df = self.__dict__[TableName.TOKENS]
        is_entry = (token_df[ColumnName.TOKEN] == token) & (
            token_df[ColumnName.SOURCE_NAME] == source_name
        )
        # Sanity
        if sum(is_entry) == 0:
            raise ValueError(
                f"{token=} cannot be found in {TableName.TOKENS}[{ColumnName.TOKEN}]"
            )
        elif sum(is_entry) > 1:
            raise ValueError(
                f"More than one {token=} found in {TableName.TOKENS}[{ColumnName.TOKEN}]"
            )
        # Make TokenInfo object (used as input in important methods)
        entry = self.__dict__[TableName.TOKENS][is_entry].iloc[0]
        token_info = TokenInfo(
            lemma=entry[ColumnName.TOKEN],
            count=entry[ColumnName.COUNT],
            source_sent_ids=entry[ColumnName.SEQS_IDS],
            source_name_str=entry[ColumnName.SOURCE_NAME],
        )
        assert token_info.source_sent_ids is not None
        # Get the dictionary entries
        token_info.dict_entries = jamdict_utils.get_dict_entries(
            query=token_info.lemma,
            drop_unfreq_entries=True,
            drop_unfreq_readings=True,
            strict_lookup=True,
        )
        # Parse the dict entries
        token_info.parsed_dict_entries = [
            jamdict_utils.parse_dict_entry(entry=entry)
            for entry in token_info.dict_entries
        ]
        # Add jj dict entry
        if token in sanseido_manipulator.sanseido_dict:
            token_info.sanseido_dict_entries = sanseido_manipulator.sanseido_dict[token]
        # Get examples
        sent_ids = token_info.source_sent_ids[
            : min(len(token_info.source_sent_ids), max_source_examples)
        ]
        source_ex_df = self.__dict__[TableName.SEQS]
        token_info.source_ex_str = [
            source_ex_df.loc[
                (source_ex_df[ColumnName.SOURCE_NAME] == source_name)
                & (source_ex_df[ColumnName.SEQ_ID] == sent_id),
                ColumnName.SEQ,
            ].iloc[0]
            for sent_id in sent_ids
        ]
        # Add tatoeba examples + translations
        if token in tatoeba_db.inverted_index:
            tatoeba_ex_idx = tatoeba_db.inverted_index[token]
            tatoeba_ex_idx = tatoeba_ex_idx[
                : min(len(tatoeba_ex_idx), max_tatoeba_examples)
            ]
            tanaka_examples = [
                tatoeba_db.tanaka_par_corpus[sent_id] for sent_id in tatoeba_ex_idx
            ]
            token_info.tatoeba_ex_str = [ex.sent_jpn for ex in tanaka_examples]
            token_info.tatoeba_ex_str_transl = [ex.sent_eng for ex in tanaka_examples]
        # Add translation
        if translate_source_ex and deepl_translator is not None:
            token_info.source_ex_str_transl = [
                deepl_translator.translate_text(  # type: ignore[union-attr]
                    text=seq, source_lang="JA", target_lang="EN-US"
                ).text
                for seq in token_info.source_ex_str
            ]
        # Make card
        cards = token_info_to_voc_cards(
            token_info=token_info,
            ex_linebreak_repl=ex_linebreak_repl,
            source_name=source_name,
        )
        # Return
        return cards

    def make_kanji_card(
        self,
        kanji: str,
        source_name: str,
    ) -> KanjiCard:
        """Make a kanji card for `kanji` from `source_name`

        Args:
            kanji (str): kanji
            source_name (str): name of the source

        Returns:
            KanjiCard
        """
        # Get the associated entry
        kanji_df = self.__dict__[TableName.KANJIS]
        is_entry = (kanji_df[ColumnName.KANJI] == kanji) & (
            kanji_df[ColumnName.SOURCE_NAME] == source_name
        )
        # Sanity
        if sum(is_entry) == 0:
            raise ValueError(
                f"{kanji=} cannot be found in {TableName.KANJIS}[{ColumnName.KANJI}]"
            )
        elif sum(is_entry) > 1:
            raise ValueError(
                f"More than one {kanji=} found in"
                f" {TableName.KANJIS}[{ColumnName.KANJI}]"
            )
        entry = self.__dict__[TableName.KANJIS][is_entry].iloc[0]
        # Get KanjiInfo
        kanji_info = jamdict_utils.get_kanji_info(kanji=kanji)
        # Add source/tokens associated to the kanji
        kanji_info.source_name = source_name
        kanji_info.seen_in_tokens = entry[ColumnName.ASSOCIATED_TOKS_FROM_SOURCE]
        # Make into KanjiCard
        kanji_card = kanji_info_to_kanji_card(kanji_info=kanji_info)
        return kanji_card

    def list_doc_names(
        self, include_hidden_in_add_full_doc_app: bool = True
    ) -> list[str]:
        """List document names in the kb

        Args:
            include_hidden_in_add_full_doc_app (bool): include documents
                hidden in the 'add full doc' app?

        Returns:
            list[str]: list of document names
        """
        docs_df = self.__dict__[TableName.DOCS]
        if include_hidden_in_add_full_doc_app:
            doc_names = docs_df[ColumnName.SOURCE_NAME].tolist()
        else:
            is_not_hidden = ~docs_df[ColumnName.HIDE_IN_ADD_FULL_DOC_APP]
            doc_names = docs_df[is_not_hidden][ColumnName.SOURCE_NAME].tolist()
        return doc_names

import copy
import datetime
from typing import Literal, Optional
import os
import pandas as pd
import logging
import deepl


from booktocards import io
from booktocards import parser
from booktocards.text import get_unique_kanjis, is_only_ascii_alphanum
from booktocards.annotations import ColName, Values
from booktocards import jamdict_utils
from booktocards.datacl import (
    TokenInfo,
    VocabCard,
    token_info_to_voc_cards,
    KanjiCard,
    kanji_info_to_kanji_card,
)
from booktocards.tatoeba import ManipulateTatoeba
from booktocards.jj_dicts import ManipulateSanseido


# ======
# Logger
# ======
logger = logging.getLogger(__name__)


# =========
# Constants
# =========
_KB_OUT_DIRNAME = "kb"
_OUT_PICKLE_EXTENSION = ".pickle"
# Data model for df in KnowledgeBase
TOKEN_TABLE_NAME = "tokens_df"
KANJI_TABLE_NAME = "kanjis_df"
SEQ_TABLE_NAME = "seqs_df"
TOKEN_COLNAME = "token"
KANJI_COLNAME = "kanji"
SEQ_COLNAME = "seq"
CARD_TABLE_NAME = "card"
COUNT_COLNAME = "count"
SEQ_ID_COLNAME = "seq_id"
SEQS_IDS_COLNAME = "seqs_ids"
SOURCE_NAME_COLNAME = "source_name"
IS_KNOWN_COLNAME = "is_known"
IS_ADDED_TO_ANKI_COLNAME = "is_added_to_anki"
IS_SUPSENDED_FOR_SOURCE_COLNAME = "is_suspended_for_source"
TO_BE_STUDIED_FROM_DATE_COLNAME = "to_be_studied_from"
ASSOCIATED_TOKS_FROM_SOURCE_COLNAME = "associated_toks_from_source"
DATA_MODEL = {  # table name: [column names]
    TOKEN_TABLE_NAME: [
        TOKEN_COLNAME,
        COUNT_COLNAME,
        SEQS_IDS_COLNAME,
        SOURCE_NAME_COLNAME,
        IS_KNOWN_COLNAME,
        IS_ADDED_TO_ANKI_COLNAME,
        IS_SUPSENDED_FOR_SOURCE_COLNAME,
        TO_BE_STUDIED_FROM_DATE_COLNAME,
    ],
    KANJI_TABLE_NAME: [
        KANJI_COLNAME,
        ASSOCIATED_TOKS_FROM_SOURCE_COLNAME,
        IS_KNOWN_COLNAME,
        IS_ADDED_TO_ANKI_COLNAME,
        IS_SUPSENDED_FOR_SOURCE_COLNAME,
        SOURCE_NAME_COLNAME,
    ],
    SEQ_TABLE_NAME: [
        SEQ_COLNAME,
        SEQ_ID_COLNAME,
        ASSOCIATED_TOKS_FROM_SOURCE_COLNAME,
        SOURCE_NAME_COLNAME,
    ],
}


# ====
# Core
# ====
# Custom exception
class NoKBError(Exception):
    """Raise when file cannot be found"""

    pass


# Path to kb
_kb_dirpath = os.path.join(
    io.get_data_path(),
    "out",
    _KB_OUT_DIRNAME,
)

# KB class
class KnowledgeBase:
    """Knowledge base for vocabulary and kanji

    At runtime, data are stored in self. At instanciation, they are loaded from
    pickle. At each operation, they are stored in pickle.

    Attributes
        One pd.DataFrame per key in `_DATA_MODEL`. The columns of dataframe p
        are the values in `_DATA_MODEL`[p]
    """

    def __init__(self):
        # Get now's timestamp for naming folders
        self.now = str(datetime.datetime.now())
        # Try load all data
        try:
            self._load_kb()
        # If impossible, initialize the db and save it
        except NoKBError:
            logger.info(f"-- No existing kb. Initilazing it.")
            # Create table
            for df_name in DATA_MODEL.keys():
                self.__dict__[df_name] = pd.DataFrame(
                    columns=DATA_MODEL[df_name]
                )
            # Cast to bool whatever needs to be
            for df_name in [TOKEN_TABLE_NAME, KANJI_TABLE_NAME]:
                for col_name in [
                    IS_KNOWN_COLNAME,
                    IS_SUPSENDED_FOR_SOURCE_COLNAME,
                ]:
                    self[df_name][col_name] = self[df_name][col_name].astype(
                        "bool"
                    )
            self.save_kb()
            logger.info(f"-- Initialized and saved")

    def __getitem__(self, arg) -> pd.DataFrame:
        """Get tables through square brakets"""
        if arg not in DATA_MODEL.keys():
            raise KeyError(f"{arg} is not one of {DATA_MODEL.keys()}")
        return self.__dict__[arg]

    def _load_df(self, df_name: str) -> None:
        """Read pd.DataFrame and attach it to self"""
        filepath = os.path.join(
            _kb_dirpath,
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
                _kb_dirpath,
                self.now,
            )
            if not os.path.isdir(dirpath):
                os.mkdir(dirpath)
        else:
            dirpath = _kb_dirpath
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

    def add_doc(
        self, doc: str, doc_name: str, drop_ascii_alphanum_toks: bool
    ) -> None:
        """Parse a document and add it's voc and sentences to kb

        Args:
            doc (str): doc
            doc_name (str): name to be used as reference in kb. Not a path or
                filename.
            drop_ascii_alphanum_toks (bool): discard tokens that are only ascii
                alphanum?
        """
        if (
            doc_name
            in self.__dict__[TOKEN_TABLE_NAME][SOURCE_NAME_COLNAME].values
            or doc_name
            in self.__dict__[KANJI_TABLE_NAME][SOURCE_NAME_COLNAME].values
            or doc_name
            in self.__dict__[SEQ_TABLE_NAME][SOURCE_NAME_COLNAME].values
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
                TOKEN_COLNAME: list(token_count_sentid.keys()),
                COUNT_COLNAME: [v[0] for v in token_count_sentid.values()],
                SEQS_IDS_COLNAME: [v[1] for v in token_count_sentid.values()],
                SOURCE_NAME_COLNAME: [
                    doc_name for i in range(len(token_count_sentid))
                ],
                IS_KNOWN_COLNAME: [
                    pd.NA for i in range(len(token_count_sentid))
                ],
                IS_ADDED_TO_ANKI_COLNAME: [
                    False for i in range(len(token_count_sentid))
                ],
                IS_SUPSENDED_FOR_SOURCE_COLNAME: [
                    False for i in range(len(token_count_sentid))
                ],
                TO_BE_STUDIED_FROM_DATE_COLNAME: [
                    None for i in range(len(token_count_sentid))
                ],
            },
            table_name=TOKEN_TABLE_NAME,
            item_colname=TOKEN_COLNAME,
        )
        # To self - kanjis
        self._add_items(
            entry_to_add={
                KANJI_COLNAME: list(uniq_kanjis_w_toks.keys()),
                ASSOCIATED_TOKS_FROM_SOURCE_COLNAME: list(
                    uniq_kanjis_w_toks.values()
                ),
                IS_KNOWN_COLNAME: [
                    pd.NA for i in range(len(uniq_kanjis_w_toks))
                ],
                IS_ADDED_TO_ANKI_COLNAME: [
                    False for i in range(len(uniq_kanjis_w_toks))
                ],
                IS_SUPSENDED_FOR_SOURCE_COLNAME: [
                    False for i in range(len(uniq_kanjis_w_toks))
                ],
                SOURCE_NAME_COLNAME: [
                    doc_name for i in range(len(uniq_kanjis_w_toks))
                ],
            },
            table_name=KANJI_TABLE_NAME,
            item_colname=KANJI_COLNAME,
        )
        # To self - extracted sequences
        self._add_items(
            entry_to_add={
                SEQ_ID_COLNAME: list(sentid_sent_toks.keys()),
                SEQ_COLNAME: [v[0] for v in sentid_sent_toks.values()],
                ASSOCIATED_TOKS_FROM_SOURCE_COLNAME: [
                    v[1] for v in sentid_sent_toks.values()
                ],
                SOURCE_NAME_COLNAME: [
                    doc_name for i in range(len(sentid_sent_toks))
                ],
            },
            table_name=SEQ_TABLE_NAME,
        )
        # Save in kb
        logger.info(f"-- Added {doc_name=} to kb.")

    def _add_items(
        self,
        entry_to_add: dict[ColName, Values],
        table_name: Literal[TOKEN_TABLE_NAME, KANJI_TABLE_NAME, SEQ_COLNAME],
        item_colname: Optional[ColName] = None,
    ) -> None:
        """Add item to the table

        Args:
            entry_to_add (dict[ColName, Values]): row to add to the table
            table_name (Literal[TOKEN_TABLE_NAME, KANJI_TABLE_NAME,
                SEQ_COLNAME]): name of the table
            item_colname (Optional[ColName]): if specified, will check that if
                the new entry shares values of `item_colname` with another
                entry, then it will take the value of IS_KNOWN_COLNAME from
                that other entry. For instance, if we add a token frm a new
                source, but that token already existed in the database and was
                marked as known, then the new entry will also be
                considered known.

        Returns:
            None
        """
        items_to_add = copy.deepcopy(entry_to_add)
        # Check keys
        if set(items_to_add.keys()) != set(
            self.__dict__[table_name].columns.to_list()
        ):
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
        # For an added row, if the value for the index exist and associated to
        # a known value, then set know to True
        if item_colname is not None:
            for obs_i, index_value in enumerate(items_to_add[item_colname]):
                table = self.__dict__[table_name]
                value_exists_and_known = (
                    table[item_colname] == index_value
                ) & (table[IS_KNOWN_COLNAME] == True)
                if any(value_exists_and_known):
                    logger.debug(
                        f"{index_value} exists in {table_name}[{item_colname}]"
                        " and is marked as known. Mark it as"
                        " know in the added items as well."
                    )
                    items_to_add[IS_KNOWN_COLNAME][obs_i] = True
        # Add the values
        self.__dict__[table_name] = pd.concat(
            [
                self.__dict__[table_name],
                pd.DataFrame(items_to_add),
            ]
        )

    def set_item_to_known(
        self,
        item_value: str,
        item_colname: ColName,
        table_name: Literal[
            TOKEN_TABLE_NAME,
            KANJI_TABLE_NAME,
        ],
    ) -> None:
        """Set item to known

        Args:
            item_value (str): value of the item in column `item_colname`
            item_colname (ColName): column on which we should look for
                `item_value`
            table_name (Literal[
                    TOKEN_TABLE_NAME,
                    KANJI_TABLE_NAME,
                ]): name of the table

        Returns:
            None:
        """
        # Find where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to known in all sources
        self.__dict__[table_name].loc[is_item, IS_KNOWN_COLNAME] = True

    def set_item_to_unknown(
        self,
        item_value: str,
        item_colname: ColName,
        table_name: Literal[
            TOKEN_TABLE_NAME,
            KANJI_TABLE_NAME,
        ],
    ) -> None:
        """Inverse of `set_item_to_known`"""
        # Find where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to unknown in all sources
        self.__dict__[table_name].loc[is_item, IS_KNOWN_COLNAME] = False

    def set_item_to_added_to_anki(
        self,
        item_value: str,
        source_name: str,
        item_colname: ColName,
        table_name: Literal[
            TOKEN_TABLE_NAME,
            KANJI_TABLE_NAME,
        ],
    ) -> None:
        """Set item as added to Anki and known

        Args:
            item_value (str): value of the item we want to set as added
            source_name (str): source of the item we want to set as added
            item_colname (ColName): column in which we will look for
                `item_value`
            table_name (Literal[
                    TOKEN_TABLE_NAME,
                    KANJI_TABLE_NAME,
                ]): name of the table

        Returns:
            None
        """
        # Find where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        is_source = (
            self.__dict__[table_name][SOURCE_NAME_COLNAME] == source_name
        )
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to added to anki in all sources
        self.__dict__[table_name].loc[
            is_item & is_source, IS_ADDED_TO_ANKI_COLNAME
        ] = True

    def set_item_to_suspended_for_source(
        self,
        item_value: str,
        source_name: str,
        item_colname: ColName,
        table_name: Literal[
            TOKEN_TABLE_NAME,
            KANJI_TABLE_NAME,
        ],
    ) -> None:
        """Set item as suspended for a given source

        Args:
            item_value (str): value of the item we want to set as suspended
            source_name (str): source of the item we want to set as suspended
            item_colname (ColName): column in which we will look for
                `item_value`
            table_name (Literal[
                    TOKEN_TABLE_NAME,
                    KANJI_TABLE_NAME,
                ]): name of the table

        Returns:
            None
        """
        # Find where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        is_source = (
            self.__dict__[table_name][SOURCE_NAME_COLNAME] == source_name
        )
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to known in all sources
        self.__dict__[table_name].loc[
            is_item & is_source, IS_SUPSENDED_FOR_SOURCE_COLNAME
        ] = True

    def set_study_from_date_for_token_source(
        self,
        token_value: str,
        source_name: str,
        date: datetime.date,
    ) -> None:
        """Set the 'study from' date for item

        The date is stored in TO_BE_STUDIED_FROM_DATE_COLNAME
        """
        # Find where TOKEN_COLNAME is equal to item_value
        is_item = self.__dict__[TOKEN_TABLE_NAME][TOKEN_COLNAME] == token_value
        is_source = (
            self.__dict__[TOKEN_TABLE_NAME][SOURCE_NAME_COLNAME] == source_name
        )
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{token_value=} cannot be found in {TOKEN_TABLE_NAME}[{TOKEN_COLNAME}]"
            )
        # Set data
        self.__dict__[TOKEN_TABLE_NAME].loc[
            is_item & is_source, TO_BE_STUDIED_FROM_DATE_COLNAME
        ] = date

    def get_items(
        self,
        table_name: Literal[
            TOKEN_TABLE_NAME,
            KANJI_TABLE_NAME,
        ],
        only_not_added: bool,
        only_not_known: bool,
        only_not_suspended: bool,
        only_no_study_date: bool,
        item_value: Optional[str] = None,
        item_colname: Optional[ColName] = None,
        source_name: Optional[str] = None,
        max_study_date: Optional[datetime.date] = None,
    ) -> pd.DataFrame:
        """Get items

        Args:
            table_name (Literal[
                    TOKEN_TABLE_NAME,
                    KANJI_TABLE_NAME,
                ]): table name
            only_not_added (bool): retrieve only those items with no True in
                IS_ADDED_TO_ANKI_COLNAME.
            only_not_known (bool): retrieve only those items with no True in
                IS_KNOWN_COLNAME (all False or NA.)
            only_not_added (bool): retrieve only those items with no True in
                IS_SUPSENDED_FOR_SOURCE_COLNAME.
            only_not_added (bool): only with no study date specified in
                TO_BE_STUDIED_FROM_DATE_COLNAME
            item_value (Optional[str]): value of the item. `item_colname` must
                be set.
            item_colname (Optional[ColName]): name of the column in which to
                look for the item.
            source_name (Optional[str]): name of the source
            max_study_date (Optional[datetime.date]): retrieve items with no
                study date of study date <= max_study_date.

        Returns:
            pd.DataFrame:
        """
        df = self.__dict__[table_name]
        # Sanity
        if only_no_study_date and max_study_date is not None:
            raise ValueError(f"{only_no_study_date=} but {max_study_date=}")
        # Identify rows
        is_items_rows = pd.Series(
            [True for _ in range(len(df))], index=df.index
        )
        if item_value is not None:
            if item_colname is None:
                raise ValueError(
                    "`item_colname` must be set when `item_value` is set."
                )
            is_items_rows = is_items_rows & (df[item_colname] == item_value)
        if source_name is not None:
            is_items_rows = is_items_rows & (
                df[SOURCE_NAME_COLNAME] == source_name
            )
        if only_not_added:
            is_items_rows = is_items_rows & (
                df[IS_ADDED_TO_ANKI_COLNAME] == False
            )
        if only_not_known:
            is_items_rows = is_items_rows & (
                (df[IS_KNOWN_COLNAME] == False) | (df[IS_KNOWN_COLNAME].isna())
            )
        if only_not_suspended:
            is_items_rows = is_items_rows & (
                df[IS_SUPSENDED_FOR_SOURCE_COLNAME] == False
            )
        if only_no_study_date:
            # No study date on kanji
            if table_name == KANJI_TABLE_NAME:
                pass
            else:
                is_items_rows = is_items_rows & (
                    df[TO_BE_STUDIED_FROM_DATE_COLNAME].isnull()
                )
        if max_study_date is not None:
            if table_name == KANJI_TABLE_NAME:
                raise ValueError(
                    "`last_study_day` was provided but is not relevant to"
                    " kanjis"
                )
            is_before_max_and_not_null = df.loc[
                is_items_rows, TO_BE_STUDIED_FROM_DATE_COLNAME
            ].apply(
                lambda x: False
                if type(x) is not datetime.date
                else x <= max_study_date
            )
            is_items_rows = is_items_rows & is_before_max_and_not_null
        return df[is_items_rows]

    def remove_doc(self, doc_name: str):
        """Remove doc from kb"""
        for table_name in DATA_MODEL.keys():
            rows_to_remove = (
                self.__dict__[table_name][SOURCE_NAME_COLNAME] == doc_name
            )
            self.__dict__[table_name] = self.__dict__[table_name][
                ~rows_to_remove
            ]
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
        deepl_translator: deepl.Translator,
    ) -> VocabCard:
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
            deepl_translator (deepl.Translator): deepl.Translator object

        Returns:
            VocabCard
        """
        # Get the associated entry
        token_df = self.__dict__[TOKEN_TABLE_NAME]
        is_entry = (token_df[TOKEN_COLNAME] == token) & (
            token_df[SOURCE_NAME_COLNAME] == source_name
        )
        # Sanity
        if sum(is_entry) == 0:
            raise ValueError(
                f"{token=} cannot be found in {TOKEN_TABLE_NAME}[{TOKEN_COLNAME}]"
            )
        elif sum(is_entry) > 1:
            raise ValueError(
                f"More than one {token=} found in {TOKEN_TABLE_NAME}[{TOKEN_COLNAME}]"
            )
        # Make TokenInfo object (used as input in important methods)
        entry = self.__dict__[TOKEN_TABLE_NAME][is_entry].iloc[0]
        token_info = TokenInfo(
            lemma=entry[TOKEN_COLNAME],
            count=entry[COUNT_COLNAME],
            source_sent_ids=entry[SEQS_IDS_COLNAME],
            source_name_str=entry[SOURCE_NAME_COLNAME],
        )
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
            token_info.sanseido_dict_entries = (
                sanseido_manipulator.sanseido_dict[token]
            )
        # Get examples
        sent_ids = token_info.source_sent_ids[
            : min(len(token_info.source_sent_ids), max_source_examples)
        ]
        source_ex_df = self.__dict__[SEQ_TABLE_NAME]
        token_info.source_ex_str = [
            source_ex_df.loc[
                (source_ex_df[SOURCE_NAME_COLNAME] == source_name)
                & (source_ex_df[SEQ_ID_COLNAME] == sent_id),
                SEQ_COLNAME,
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
                tatoeba_db.tanaka_par_corpus[sent_id]
                for sent_id in tatoeba_ex_idx
            ]
            token_info.tatoeba_ex_str = [ex.sent_jpn for ex in tanaka_examples]
            token_info.tatoeba_ex_str_transl = [
                ex.sent_eng for ex in tanaka_examples
            ]
        # Add translation
        if translate_source_ex:
            token_info.source_ex_str_transl = [
                deepl_translator.translate_text(
                    text=seq, source_lang="JA", target_lang="EN-US"
                ).text
                for seq in token_info.source_ex_str
            ]
        # Make card
        cards = token_info_to_voc_cards(
            token_info=token_info, source_name=source_name
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
        kanji_df = self.__dict__[KANJI_TABLE_NAME]
        is_entry = (kanji_df[KANJI_COLNAME] == kanji) & (
            kanji_df[SOURCE_NAME_COLNAME] == source_name
        )
        # Sanity
        if sum(is_entry) == 0:
            raise ValueError(
                f"{kanji=} cannot be found in {KANJI_TABLE_NAME}[{KANJI_COLNAME}]"
            )
        elif sum(is_entry) > 1:
            raise ValueError(
                f"More than one {kanji=} found in"
                f" {KANJI_TABLE_NAME}[{KANJI_COLNAME}]"
            )
        entry = self.__dict__[KANJI_TABLE_NAME][is_entry].iloc[0]
        # Get KanjiInfo
        kanji_info = jamdict_utils.get_kanji_info(kanji=kanji)
        # Add source/tokens associated to the kanji
        kanji_info.source_name = source_name
        kanji_info.seen_in_tokens = entry[ASSOCIATED_TOKS_FROM_SOURCE_COLNAME]
        # Make into KanjiCard
        kanji_card = kanji_info_to_kanji_card(kanji_info=kanji_info)
        return kanji_card


# TODO: remove
# from booktocards.jj_dicts import ManipulateSanseido
# from booktocards.tatoeba import ManipulateTatoeba
# kb = KnowledgeBase()
# card_1 = kb.make_voc_cards(token="名前", source_name="A1p",
#            max_source_examples=2,
#            max_tatoeba_examples=2,
#            translate_source_ex=False,
#    sanseido_manipulator=ManipulateSanseido(),
#    tatoeba_db=ManipulateTatoeba(),
#    deepl_translator=None,
# )

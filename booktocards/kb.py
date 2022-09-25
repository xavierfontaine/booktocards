import copy
from typing import Literal, Optional
import os
import pandas as pd
import logging
import deepl


from booktocards import io
from booktocards import parser
from booktocards.text import get_unique_kanjis
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
_OUT_JSON_EXTENSIONS = ".json"
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
    json. At each operation, they are stored in json.

    Attributes
        One pd.DataFrame per key in `_DATA_MODEL`. The columns of dataframe p
        are the values in `_DATA_MODEL`[p]
    """

    def __init__(self):
        # Try load all data, and if impossible, initialize
        try:
            self._load_kb()
        except NoKBError:
            logger.info(f"-- No existing kb. Initilazing it.")
            for df_name in DATA_MODEL.keys():
                self.__dict__[df_name] = pd.DataFrame(
                    columns=DATA_MODEL[df_name]
                )
            self._save_df(df_name=df_name)
            logger.info(f"-- Initialized")

    def __getitem__(self, arg) -> pd.DataFrame:
        """Get tables through square brakets"""
        if arg not in DATA_MODEL.keys():
            raise KeyError(f"{arg} is not one of {DATA_MODEL.keys()}")
        return self.__dict__[arg]

    def _load_df(self, df_name: str) -> None:
        """Read pd.DataFrame and attach it to self"""
        filepath = os.path.join(
            _kb_dirpath,
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
            _kb_dirpath,
            df_name + _OUT_JSON_EXTENSIONS,
        )
        with open(filepath, "w") as f:
            self.__dict__[df_name].to_json(
                path_or_buf=f, orient="table", index=False
            )

    def _load_kb(self) -> None:
        """Load the kb frm jsons"""
        for df_name in DATA_MODEL.keys():
            self._load_df(df_name=df_name)

    def _save_kb(self) -> None:
        """Save the kb into several jsons (1 per table)"""
        for df_name in DATA_MODEL.keys():
            self._save_df(df_name=df_name)

    def add_doc(self, doc: str, doc_name: str) -> None:
        """Parse a document and add it's voc and sentences to kb

        `doc_name` is not path-related, and rather used as reference in the kb."""
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
                IS_ADDED_TO_ANKI_COLNAME: [
                    False for i in range(len(token_count_sentid))
                ],
                IS_KNOWN_COLNAME: [
                    False for i in range(len(token_count_sentid))
                ],
                IS_SUPSENDED_FOR_SOURCE_COLNAME: [
                    False for i in range(len(token_count_sentid))
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
                    False for i in range(len(uniq_kanjis_w_toks))
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
        self._save_kb()
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
        # Finf where item_colname is equal to item_value
        is_item = self.__dict__[table_name][item_colname] == item_value
        # Sanity
        if not any(is_item):
            raise ValueError(
                f"{item_value=} cannot be found in {table_name}[{item_colname}]"
            )
        # Set to known in all sources
        self.__dict__[table_name].loc[is_item, IS_KNOWN_COLNAME] = True
        # Save
        self._save_kb()

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
        # Save
        self._save_kb()

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
        """Set item as added to Anki

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
        # Finf where item_colname is equal to item_value
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
            is_item & is_source, IS_ADDED_TO_ANKI_COLNAME
        ] = True
        # Save
        self._save_kb()

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
        # Finf where item_colname is equal to item_value
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
        # Save
        self._save_kb()

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
        self._save_kb()

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
        kanji_info.seen_in_source = source_name
        kanji_info.seen_in_tokens = entry[ASSOCIATED_TOKS_FROM_SOURCE_COLNAME]
        # Make into KanjiCard
        kanji_card = kanji_info_to_kanji_card(kanji_info=kanji_info)
        return kanji_card

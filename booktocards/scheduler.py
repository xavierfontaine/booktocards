"""
Scheduling of studies
"""
import copy
from typing import Literal
import datetime
from datetime import date, timedelta
from deepl import Translator
import os
import pandas as pd
from typing import Optional

from booktocards import io
from booktocards.annotations import Token, Kanji, SourceName, FilePath
from booktocards.datacl import VocabCard, KanjiCard
from booktocards.kb import KnowledgeBase
from booktocards.kb import (
    DATA_MODEL,
    TOKEN_TABLE_NAME,
    KANJI_TABLE_NAME,
    SEQ_TABLE_NAME,
    SOURCE_NAME_COLNAME,
    TOKEN_COLNAME,
    KANJI_COLNAME,
    SEQ_COLNAME,
    TO_BE_STUDIED_FROM_DATE_COLNAME,
    COUNT_COLNAME,
)
from booktocards.jj_dicts import ManipulateSanseido
from booktocards.tatoeba import ManipulateTatoeba
from booktocards.text import get_unique_kanjis


# TODO: nothing should be set as known, added, suspendend, or have a due date
# until end_scheduling is called.
# Instead, there should be methods to check whether token/kanji is in one of
# the self tables and/or previously marked a known/added etc.
# =========
# Constants
# =========
_CARDS_DIRNAME = "cards"


# ==========
# Exceptions
# ==========
class EnoughItemsAddedError(Exception):
    """Raise when file cannot be found"""

    pass


class NoAddableEntryError(Exception):
    """Cannot find an entry that isn't known, suspendehd or added"""

    pass


class KanjiNotKnownError(Exception):
    """The associated kanji isn't marked a known"""

    pass


class KanjiNotKnownOrAddedError(Exception):
    """The associated kanji isn't marked a known"""

    pass


class AlreadyAddedError(Exception):
    """The item was already added to the scheduler"""

    pass


class UncertainVocRemainError(Exception):
    """Tried to perform operation that is illegal when
    vocab_w_uncertain_status_df isn't empty"""

    pass


# ====
# Core
# ====
_cards_dirpath = os.path.join(
    io.get_data_path(),
    "out",
    _CARDS_DIRNAME,
)


class Scheduler:
    # TODO: docstr
    # Works only at the item level, not card level. Meaning when can end up
    # with more cards than expected

    def __init__(
        self,
        kb: KnowledgeBase,
        n_days_study: int,
        n_cards_days: int,
        min_days_btwn_kanji_and_voc: int,
    ):
        # Sanity
        if n_days_study >= min_days_btwn_kanji_and_voc:
            raise ValueError(
                "Choose a `n_days_study` shorter than"
                " `min_time_btwn_kanji_and_voc`"
            )
        # attach passed argument to self
        self.kb = kb
        self.n_days_study = n_days_study
        self.n_cards_days = n_cards_days
        self.min_time_btwn_kanji_and_voc = min_days_btwn_kanji_and_voc
        self.today = datetime.date.today()
        # Init df for vocab with possibly unknown kanji
        self.vocab_w_uncertain_status_df = pd.DataFrame(
            columns=DATA_MODEL[TOKEN_TABLE_NAME]
        )
        # Init df for newly added vocab (not due) that will go into next round
        self.vocab_for_next_round_df = pd.DataFrame(
            columns=DATA_MODEL[TOKEN_TABLE_NAME]
        )
        # Init df for newly added kanji that will go into next round
        self.kanji_for_next_round_df = pd.DataFrame(
            columns=DATA_MODEL[KANJI_TABLE_NAME]
        )
        # Init df for newly added vocab (not due) that will go into next round
        self.vocab_for_rounds_after_next_df = pd.DataFrame(
            columns=DATA_MODEL[TOKEN_TABLE_NAME]
        )
        # Init df for voc/kanji the user asked to add to known or suspended
        self.vocab_set_to_add_to_known: list[Token] = list()
        self.kanji_set_to_add_to_known: list[Kanji] = list()
        self.vocab_set_to_add_to_suspended: list[
            list[Token, SourceName]
        ] = list()
        self.kanji_set_to_add_to_suspended: list[
            list[Kanji, SourceName]
        ] = list()
        # Get due vocab
        self.due_vocab_df = self.get_due_vocab()
        self.not_known_added_kanji_df = self.kb.get_items(
            table_name=KANJI_TABLE_NAME,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=True,
            only_no_study_date=True,
        )
        # Add due vocab as much as possible
        max_due_vocab_to_add = min(
            len(self.due_vocab_df), self.n_cards_days * self.n_days_study
        )
        for i in range(max_due_vocab_to_add):
            idx = self.due_vocab_df.index[i]
            self.add_vocab_for_next_round(
                token=self.due_vocab_df.loc[idx, TOKEN_COLNAME],
                source_name=self.due_vocab_df.loc[idx, SOURCE_NAME_COLNAME],
            )
        # Calculate number of vocab to add
        self.n_items_to_add = (
            self.n_days_study * self.n_cards_days
        )  - len(self.due_vocab_df) 

    def set_vocab_to_add_to_known(
        self,
        token: Token,
    ):
        """Set vocab to be added as known when running `end_scheduling`"""
        self.vocab_set_to_add_to_known.append(token)

    def set_kanji_to_add_to_known(
        self,
        kanji: Kanji,
    ):
        """Set kanji to be added as known when running `end_scheduling`"""
        self.kanji_set_to_add_to_known.append(kanji)

    def set_vocab_to_add_to_suspended(
        self,
        token: Token,
        source_name: SourceName,
    ):
        """Set vocab to be added as suspended when running `end_scheduling`"""
        self.vocab_set_to_add_to_suspended.append([token, source_name])

    def set_kanji_to_add_to_suspended(
        self,
        kanji: Kanji,
        source_name: SourceName,
    ):
        """Set kanji to be added as suspended when running `end_scheduling`"""
        self.kanji_set_to_add_to_suspended.append([kanji, source_name])

    def get_due_vocab(self) -> pd.DataFrame:
        """Get due vocabulary items

        Due vocabulary items are those
        - with a date attribute stating from when they should be studied,
        - such that that date for starting the study is earlier than today +
          self.n_days_study
        """
        # TODO: finish docstr (output: extrait de kb[TOKEN_TABLE_NAME])
        max_day_study = self.today + timedelta(days=self.n_days_study)
        token_df = self.kb.get_items(
            table_name=TOKEN_TABLE_NAME,
            item_value=None,
            item_colname=None,
            source_name=None,
            only_no_study_date=False,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=True,
            max_study_date=max_day_study,
        )
        return token_df

    def _raise_error_if_tok_in_df(self, token: Token, df_name: str):
        df = getattr(self, df_name)
        if token in df[TOKEN_COLNAME].tolist():
            raise AlreadyAddedError(
                f"{token=} is already added to self.{df_name}"
            )

    def get_studiable_voc(self, sort: bool = False) -> pd.DataFrame:
        """Return voc not marked in either the kb and the scheduler

        Not marked in kb as known/added/suspended/due

        Not marked in self as to be added as known/suspended, or as vocab to be
        added to any round or with uncertain status
        """
        # Start with tokens not marked in db
        token_not_marked_in_kb_df = self.kb.get_items(
            table_name=TOKEN_TABLE_NAME,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=True,
            only_no_study_date=True,
        )
        # Init the bool for tokens not marked in self
        is_marked_in_sched = [
            False for i in range(len(token_not_marked_in_kb_df))
        ]
        # In vocab_w_uncertain_status_df?
        is_marked_in_sched = is_marked_in_sched | (
            token_not_marked_in_kb_df[TOKEN_COLNAME].isin(
                self.vocab_w_uncertain_status_df[TOKEN_COLNAME]
            )
        )
        # In vocab_for_next_round_df?
        is_marked_in_sched = is_marked_in_sched | (
            token_not_marked_in_kb_df[TOKEN_COLNAME].isin(
                self.vocab_for_next_round_df[TOKEN_COLNAME]
            )
        )
        # In vocab_for_rounds_after_next_df?
        is_marked_in_sched = is_marked_in_sched | (
            token_not_marked_in_kb_df[TOKEN_COLNAME].isin(
                self.vocab_for_rounds_after_next_df[TOKEN_COLNAME]
            )
        )
        # In vocab_set_to_add_to_known_df?
        is_marked_in_sched = is_marked_in_sched | (
            token_not_marked_in_kb_df[TOKEN_COLNAME].isin(
                self.vocab_set_to_add_to_known
            )
        )
        # In vocab_set_to_add_to_suspended_df?
        is_marked_in_sched = is_marked_in_sched | (
            token_not_marked_in_kb_df[TOKEN_COLNAME].isin(
                [tok for tok, source in self.vocab_set_to_add_to_suspended]
            )
        )
        # Get out df
        out_df = token_not_marked_in_kb_df[~is_marked_in_sched]
        # Sort
        if sort:
            out_df = out_df.sort_values(by=[COUNT_COLNAME],
            ascending=False)
        return out_df

    def get_studiable_kanji(self) -> pd.DataFrame:
        """Return kanji not marked in either the kb and the scheduler

        Not marked in kb as known/added/suspended/due

        Not marked in self as to be added as known/suspended, or as kanji to be
        added to any round or with uncertain status
        """
        # Start with kanji not marked in db
        kanji_not_marked_in_kb_df = self.kb.get_items(
            table_name=KANJI_TABLE_NAME,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=True,
            only_no_study_date=True,
        )
        # Init the bool for kanji not marked in self
        is_marked_in_sched = [
            False for i in range(len(kanji_not_marked_in_kb_df))
        ]
        # In kanji_for_next_round_df?
        is_marked_in_sched = is_marked_in_sched | (
            kanji_not_marked_in_kb_df[KANJI_COLNAME].isin(
                self.kanji_for_next_round_df[KANJI_COLNAME]
            )
        )
        # In kanji_set_to_add_to_known_df?
        is_marked_in_sched = is_marked_in_sched | (
            kanji_not_marked_in_kb_df[KANJI_COLNAME].isin(
                self.kanji_set_to_add_to_known
            )
        )
        # In kanji_set_to_add_to_suspended_df?
        is_marked_in_sched = is_marked_in_sched | (
            kanji_not_marked_in_kb_df[KANJI_COLNAME].isin(
                [tok for tok, source in self.kanji_set_to_add_to_suspended]
            )
        )
        # Return
        return kanji_not_marked_in_kb_df[~is_marked_in_sched]

    def _raise_error_if_tok_in_uncertain_df(self, token: Token):
        """_raise_error_if_tok_in_uncertain_df.

        Args:
            token (Token): token
        """
        self._raise_error_if_tok_in_df(
            token=token, df_name="vocab_w_uncertain_status_df"
        )

    def _raise_error_if_tok_in_voc_for_next_round(self, token: Token):
        self._raise_error_if_tok_in_df(
            token=token, df_name="vocab_for_next_round_df"
        )

    def _raise_error_if_tok_in_voc_for_rounds_after_next(self, token: Token):
        self._raise_error_if_tok_in_df(
            token=token, df_name="vocab_for_rounds_after_next_df"
        )

    def _raise_error_if_kanji_in_added_kanji(self, kanji: Kanji):
        df_name = "kanji_for_next_round_df"
        df = getattr(self, df_name)
        if kanji in df[KANJI_COLNAME].tolist():
            raise AlreadyAddedError(
                f"{kanji=} is already added to self.{df_name}"
            )

    def _raise_error_if_too_much_items_already_present(
        self, item_value, source_name: SourceName
    ):
        n_added_items = (
            len(self.kanji_for_next_round_df)
            + len(self.vocab_for_next_round_df)
            + len(self.vocab_w_uncertain_status_df)
        )
        max_added_items = self.n_days_study * self.n_cards_days
        if n_added_items >= max_added_items:
            raise EnoughItemsAddedError(
                f"Already {n_added_items}/{max_added_items} added items for"
                " next study."
                f" Not adding {item_value=} for {source_name=}."
            )

    def add_vocab_of_interest(self, token: Token, source_name: SourceName):
        """Select vocabulary to be studied in any future round

        If all kanjis are known, ad to
        self.new_vocab_for_next_round_df_df. Else, add to
        self.vocab_w_uncertain_status_df
        """
        # TODO: finish docstr
        # Sanity
        self._raise_error_if_tok_in_uncertain_df(token=token)
        self._raise_error_if_tok_in_voc_for_next_round(token=token)
        self._raise_error_if_tok_in_voc_for_rounds_after_next(token=token)
        self._raise_error_if_too_much_items_already_present(
            item_value=token, source_name=source_name
        )
        # Refuse if already enough items added
        n_added_items = len(self.kanji_for_next_round_df) + len(
            self.vocab_w_uncertain_status_df
        )
        max_added_items = self.n_days_study * self.n_cards_days
        if n_added_items >= max_added_items:
            raise EnoughItemsAddedError(
                f"Already {n_added_items}/{max_added_items} added items for"
                " next study."
                f" Not adding {token=} for {source_name=}."
            )
        # Get item from kb
        token_df = self.kb.get_items(
            table_name=TOKEN_TABLE_NAME,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=True,
            only_no_study_date=True,
            item_value=token,
            item_colname=TOKEN_COLNAME,
            source_name=source_name,
            max_study_date=None,
        )
        if len(token_df) == 0:
            raise NoAddableEntryError(
                f"No entry in the kb for {token=} and {source_name=} among"
                " those that are not known, added, suspended or with study"
                " date."
            )
        # For each row:
        for idx in token_df.index:
            # Get kanjis that are not known or added
            kanji_not_known_added_schedtoknown = self.get_kanjis_sources_from_token_df(
                token_df=token_df.loc[idx].to_frame().T,
                only_not_added=True,
                only_not_known=True,
                only_not_suspended=False,
                only_not_sched_to_added=False,
                only_not_sched_to_known=True,
                only_not_sched_to_suspended=False,
            )[KANJI_COLNAME].tolist()
            # If all kanjis are known, trigger add_vocab_for_next_round
            if len(kanji_not_known_added_schedtoknown) == 0:
                self.add_vocab_for_next_round(
                    token=token_df.loc[idx, TOKEN_COLNAME],
                    source_name=token_df.loc[idx, SOURCE_NAME_COLNAME],
                )
            # Else, add to self.vocab_w_uncertain_status_df
            else:
                self.vocab_w_uncertain_status_df = pd.concat(
                    [
                        self.vocab_w_uncertain_status_df,
                        token_df.loc[idx].to_frame().T,
                    ]
                )

    def add_vocab_for_next_round(self, token: Token, source_name: SourceName):
        """Add vocab to next study cycle"""
        # Sanity
        self._raise_error_if_tok_in_voc_for_next_round(token=token)
        self._raise_error_if_tok_in_voc_for_rounds_after_next(token=token)
        # Check vocab in uncertain df
        if token in self.vocab_w_uncertain_status_df[TOKEN_COLNAME].tolist():
            vocab_is_in_uncertain_df = True
        else:
            vocab_is_in_uncertain_df = False
        # Check not to much vocab already, except if voc present in
        # vocab_w_uncertain_status_df (will be promoted to
        # vocab_for_next_round_df)
        if not vocab_is_in_uncertain_df:
            self._raise_error_if_too_much_items_already_present(
                item_value=token, source_name=source_name
            )
        # Get tokens
        token_df = self.kb.get_items(
            table_name=TOKEN_TABLE_NAME,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=True,
            only_no_study_date=True,
            item_value=token,
            item_colname=TOKEN_COLNAME,
            source_name=source_name,
            max_study_date=None,
        )
        assert len(token_df) == 1
        # Refuse if kanji not marked as known
        kanji_not_known_added_schedtoknown = self.get_kanjis_sources_from_token_df(
            token_df=token_df,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=False,
            only_not_sched_to_added=False,
            only_not_sched_to_known=True,
            only_not_sched_to_suspended=False,
        )[KANJI_COLNAME].tolist()
        if len(kanji_not_known_added_schedtoknown) != 0:
            raise KanjiNotKnownError(
                f"Some kanjis in {token} are not known:"
                f"\n{kanji_not_known_added_schedtoknown}"
            )
        # Otherwise, add to vocab for next round
        self.vocab_for_next_round_df = pd.concat(
            [
                self.vocab_for_next_round_df,
                token_df,
            ]
        )
        # If was in vocab_w_uncertain_status_df, remove
        if vocab_is_in_uncertain_df:
            self._remove_from_uncertain_vocab_df(
                token=token, source_name=source_name
            )

    def add_kanji_for_next_round(self, kanji: Kanji, source_name: SourceName):
        # TODO: docstr
        # Sanity
        self._raise_error_if_kanji_in_added_kanji(kanji=kanji)
        self._raise_error_if_too_much_items_already_present(
            item_value=kanji, source_name=source_name
        )
        # Refuse if already enough items added
        n_added_items = len(self.kanji_for_next_round_df) + len(
            self.vocab_for_next_round_df
        )
        max_added_items = self.n_days_study * self.n_cards_days
        if n_added_items >= max_added_items:
            raise ValueError(
                f"Already {n_added_items}/{max_added_items} added items for"
                " next study."
                f" Not adding {kanji=} for {source_name=}."
            )
        # Get kanji
        kanji_df = self.kb.get_items(
            table_name=KANJI_TABLE_NAME,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=True,
            only_no_study_date=True,
            item_value=kanji,
            item_colname=KANJI_COLNAME,
            source_name=source_name,
            max_study_date=None,
        )
        # Add to kanji for next round
        self.kanji_for_next_round_df = pd.concat(
            [
                self.kanji_for_next_round_df,
                kanji_df,
            ]
        )

    def add_vocab_for_rounds_after_next(
        self, token: Token, source_name: SourceName
    ):
        """Add vocab for a future round

        Two actions:
        - Set due take for token in kb
        - Remove from list of tokens with uncertain study status
        """
        # Sanity
        self._raise_error_if_tok_in_voc_for_next_round(token=token)
        self._raise_error_if_tok_in_voc_for_rounds_after_next(token=token)
        # Check is in self.vocab_w_uncertain_status_df
        in_vocab_w_uncertain_status_df = (
            self.vocab_w_uncertain_status_df[TOKEN_COLNAME] == token
        ) & (
            self.vocab_w_uncertain_status_df[SOURCE_NAME_COLNAME]
            == source_name
        )
        if len(in_vocab_w_uncertain_status_df) == 0:
            raise NoAddableEntryError(
                "Cannot add vocab for round after next if not in"
                f" `self.vocab_w_uncertain_status_df`. Tried to add {token=}"
                f" for {source_name}, but {self.vocab_w_uncertain_status_df=}."
            )
        # Get tokens
        token_df = self.kb.get_items(
            table_name=TOKEN_TABLE_NAME,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=True,
            only_no_study_date=True,
            item_value=token,
            item_colname=TOKEN_COLNAME,
            source_name=source_name,
            max_study_date=None,
        )
        assert len(token_df) == 1
        # Check all kanjis are marked as know or belong to self.kanji_for_next_round_df
        kanji_not_known_added_schedtoadded_schedtoknown = self.get_kanjis_sources_from_token_df(
            token_df=token_df,
            only_not_added=True,
            only_not_known=True,
            only_not_suspended=False,
            only_not_sched_to_added=True,
            only_not_sched_to_known=True,
            only_not_sched_to_suspended=False,
        )[KANJI_COLNAME].tolist()
        kanji_added_ls = self.kanji_for_next_round_df[KANJI_COLNAME].tolist()
        if not set(kanji_not_known_added_schedtoadded_schedtoknown).issubset(
            set(kanji_added_ls)
        ):
            raise KanjiNotKnownOrAddedError(
                f"For {token=}, some kanjis are not known, and yet not added to"
                " the list of kanjis to learn."
                f"\n{kanji_not_known_added_schedtoadded_schedtoknown=}."
                f"\n{kanji_added_ls=}."
            )
        # Remove token from self.vocab_w_uncertain_status_df
        self._remove_from_uncertain_vocab_df(
            token=token, source_name=source_name
        )
        # Add study date
        date = self.today + timedelta(days=self.min_time_btwn_kanji_and_voc)
        token_df[TO_BE_STUDIED_FROM_DATE_COLNAME] = date
        # Add token to self.vocab_for_rounds_after_next_df
        self.vocab_for_rounds_after_next_df = pd.concat(
            [
                self.vocab_for_rounds_after_next_df,
                token_df,
            ]
        )

    def _remove_from_uncertain_vocab_df(
        self,
        token: Token,
        source_name: SourceName,
    ) -> None:
        """Remove token for source_name frm self.vocab_w_uncertain_status_df"""
        uncertain_df = self.vocab_w_uncertain_status_df
        is_token_source = (uncertain_df[TOKEN_COLNAME] == token) & (
            uncertain_df[SOURCE_NAME_COLNAME] == source_name
        )
        self.vocab_w_uncertain_status_df = uncertain_df[~is_token_source]

    def get_kanjis_sources_from_token_df(
        self,
        token_df: pd.DataFrame,
        only_not_added: bool,
        only_not_known: bool,
        only_not_suspended: bool,
        only_not_sched_to_added: bool,
        only_not_sched_to_known: bool,
        only_not_sched_to_suspended: bool,
    ) -> pd.DataFrame:
        """Extract [kanji, source] couples from all tokens"""
        kanjis_sources_df = pd.DataFrame()
        for token, source_name in token_df[
            [TOKEN_COLNAME, SOURCE_NAME_COLNAME]
        ].values:
            kanjis = get_unique_kanjis(doc=token)
            for kanji in kanjis:
                kanji_source_df = self.kb.get_items(
                    table_name=KANJI_TABLE_NAME,
                    only_not_added=only_not_added,
                    only_not_known=only_not_known,
                    only_not_suspended=only_not_suspended,
                    only_no_study_date=True,
                    item_value=kanji,
                    item_colname=KANJI_COLNAME,
                    source_name=source_name,
                    max_study_date=None,
                )
                kanjis_sources_df = pd.concat(
                    [kanjis_sources_df, kanji_source_df]
                )
        kanjis_sources_df = kanjis_sources_df.drop_duplicates(
            subset=[KANJI_COLNAME, SOURCE_NAME_COLNAME]
        )
        if only_not_sched_to_added:
            kanjis_sources_df = kanjis_sources_df[
                ~kanjis_sources_df[KANJI_COLNAME].isin(self.kanji_for_next_round_df[KANJI_COLNAME])
            ]
        if only_not_sched_to_known:
            kanjis_sources_df = kanjis_sources_df[
                ~kanjis_sources_df[KANJI_COLNAME].isin(self.kanji_set_to_add_to_known)
            ]
        if only_not_sched_to_suspended:
            kanjis_sources_df = kanjis_sources_df[
                ~kanjis_sources_df[KANJI_COLNAME].isin([token for token,
                    source_name in self.kanji_set_to_add_to_suspended
            ])]
        return kanjis_sources_df

    def _make_voc_cards_from_query(
        self,
        token: str,
        source_name: str,
        translate_source_ex: bool,
        sanseido_manipulator: ManipulateSanseido,
        tatoeba_db: ManipulateTatoeba,
        max_source_examples: int = 3,
        max_tatoeba_examples: int = 3,
        deepl_translator: Optional[Translator] = None,
    ) -> list[VocabCard]:
        """Wrapper for code legibility"""
        kb = self.kb
        cards = kb.make_voc_cards(
            token=token,
            source_name=source_name,
            translate_source_ex=False,
            max_source_examples=max_source_examples,
            max_tatoeba_examples=max_tatoeba_examples,
            sanseido_manipulator=sanseido_manipulator,
            tatoeba_db=tatoeba_db,
            deepl_translator=deepl_translator,
        )
        return cards

    def make_voc_cards_from_df(
        self,
        token_df: pd.DataFrame,
        translate_source_ex: bool,
        token_colname: str,
        source_name_colname: str,
        sanseido_manipulator: ManipulateSanseido,
        tatoeba_db: ManipulateTatoeba,
        deepl_translator: Optional[Translator] = None,
    ) -> list[VocabCard]:
        """Wrapper for code legibility"""
        cards = []
        for token, source_name in token_df[
            [token_colname, source_name_colname]
        ].values:
            card = self._make_voc_cards_from_query(
                token=token,
                source_name=source_name,
                translate_source_ex=False,
                sanseido_manipulator=sanseido_manipulator,
                tatoeba_db=tatoeba_db,
                deepl_translator=deepl_translator,
            )
            cards.extend(card)
        return cards

    def make_kanji_cards_from_df(
        self,
        kanji_df: pd.DataFrame,
    ) -> list[KanjiCard]:
        """Make kanji cards from a df containing KANJI_COLNAME and
        SOURCE_NAME_COLNAME"""
        cards = []
        for kanji, source_name in kanji_df[
            [KANJI_COLNAME, SOURCE_NAME_COLNAME]
        ].values:
            card = self.kb.make_kanji_card(
                kanji=kanji, source_name=source_name
            )
            cards.append(card)
        return cards

    def end_scheduling(
        self,
        translate_source_ex: bool,
        sanseido_manipulator: ManipulateSanseido,
        tatoeba_db: ManipulateTatoeba,
        deepl_translator: Optional[Translator] = None,
    ) -> dict[Literal["vocab", "kanji"], FilePath]:
        """Write cards and make backup"""
        # Make a copy of db before modifying it
        altered_kb = copy.deepcopy(self.kb)
        ## Check no vocab si in vocab_w_uncertain_status_df
        # if len(self.vocab_w_uncertain_status_df) > 0:
        #    raise UncertainVocRemainError(
        #        "Items remain on self.vocab_w_uncertain_status_df:"
        #        f"\n{self.vocab_w_uncertain_status_df}"
        #    )
        # Make cards
        vocab_cards_df = pd.DataFrame(
            self.make_voc_cards_from_df(
                token_df=self.vocab_for_next_round_df,
                translate_source_ex=translate_source_ex,
                token_colname=TOKEN_COLNAME,
                source_name_colname=SOURCE_NAME_COLNAME,
                sanseido_manipulator=sanseido_manipulator,
                tatoeba_db=tatoeba_db,
                deepl_translator=deepl_translator,
            )
        )
        kanji_cards_df = pd.DataFrame(
            self.make_kanji_cards_from_df(
                kanji_df=self.kanji_for_next_round_df,
            )
        )
        # Mark vocab added for next round as added
        self.vocab_for_next_round_df.apply(
            lambda x: altered_kb.set_item_to_added_to_anki(
                item_value=x[TOKEN_COLNAME],
                source_name=x[SOURCE_NAME_COLNAME],
                item_colname=TOKEN_COLNAME,
                table_name=TOKEN_TABLE_NAME,
            ),
            axis=1,
        )
        # Add due date to vocab that has due date
        self.vocab_for_rounds_after_next_df.apply(
            lambda x: altered_kb.set_study_from_date_for_token_source(
                token_value=x[TOKEN_COLNAME],
                source_name=x[SOURCE_NAME_COLNAME],
                date=x[TO_BE_STUDIED_FROM_DATE_COLNAME],
            ),
            axis=1,
        )
        # Mark as known vocab tagged as "to known"
        for token in self.vocab_set_to_add_to_known:
            altered_kb.set_item_to_known(
                item_value=token,
                item_colname=TOKEN_COLNAME,
                table_name=TOKEN_TABLE_NAME,
            )
        # Mark as suspended vocab tagged as "to suspend"
        for token, source_name in self.vocab_set_to_add_to_suspended:
            altered_kb.set_item_to_suspended_for_source(
                item_value=token,
                source_name=source_name,
                item_colname=TOKEN_COLNAME,
                table_name=TOKEN_TABLE_NAME,
            )
        # Mark kanji added for next round as added
        self.kanji_for_next_round_df.apply(
            lambda x: altered_kb.set_item_to_added_to_anki(
                item_value=x[KANJI_COLNAME],
                source_name=x[SOURCE_NAME_COLNAME],
                item_colname=KANJI_COLNAME,
                table_name=KANJI_TABLE_NAME,
            ),
            axis=1,
        )
        # Mark as known kanji tagged as "to known"
        for kanji in self.kanji_set_to_add_to_known:
            altered_kb.set_item_to_known(
                item_value=kanji,
                item_colname=KANJI_COLNAME,
                table_name=KANJI_TABLE_NAME,
            )
        # Mark as suspendehd kanji tagged as "to suspended"
        for kanji, source_name in self.kanji_set_to_add_to_suspended:
            altered_kb.set_item_to_suspended_for_source(
                item_value=kanji,
                source_name=source_name,
                item_colname=KANJI_COLNAME,
                table_name=KANJI_TABLE_NAME,
            )
        # Make folder to write cards
        now = str(datetime.datetime.now())
        out_folder = os.path.join(
            _cards_dirpath,
            now,
        )
        os.mkdir(out_folder)
        # Write cards to xlsx
        vocab_filepath = os.path.join(out_folder, "vocab.xlsx")
        kanji_filepath = os.path.join(out_folder, "kanji.xlsx")
        vocab_cards_df.to_csv(
            vocab_filepath,
            index=False,
        )
        kanji_cards_df.to_csv(
            kanji_filepath,
            index=False,
        )
        # Save db with backup
        altered_kb.save_kb(make_backup=True)
        return {
            "vocab": vocab_filepath,
            "kanji": kanji_filepath,
        }
        # TODO: add self-destroy mechanism

"""
Test level: integration (requires db, sanseido, jj to work well)
"""
import os
import pandas as pd
import pytest
from unittest.mock import Mock
from datetime import timedelta

from booktocards.kb import (
    TOKEN_TABLE_NAME,
    KANJI_TABLE_NAME,
    TOKEN_COLNAME,
    KANJI_COLNAME,
    SOURCE_NAME_COLNAME,
    TO_BE_STUDIED_FROM_DATE_COLNAME,
    IS_KNOWN_COLNAME,
    IS_ADDED_TO_ANKI_COLNAME,
    IS_SUPSENDED_FOR_SOURCE_COLNAME,
)
import booktocards.kb
import booktocards.scheduler
from booktocards.scheduler import (
    NoAddableEntryError,
    EnoughItemsAddedError,
    KanjiNotKnownError,
    KanjiNotKnownOrAddedError,
    UncertainVocRemainError,
)
from booktocards.tatoeba import ManipulateTatoeba
from booktocards.jj_dicts import ManipulateSanseido


def test_add_voc_with_known_kanjis(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    min_days_btwn_kanji_and_voc = 3
    scheduler = booktocards.scheduler.Scheduler(
        kb=kb,
        n_days_study=2,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    # Try to put as "of interest" a voc that's already marked as known
    doc = "食べる飲む歌う。歌う。"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    with pytest.raises(NoAddableEntryError):
        scheduler.add_vocab_of_interest(token="食べる", source_name=source_name)
    # Put as "for next round" a word for which kanji is clearly known
    kb.set_item_to_known(
        item_value="飲",
        item_colname=KANJI_COLNAME,
        table_name=KANJI_TABLE_NAME,
    )
    scheduler.add_vocab_for_next_round(token="飲む", source_name=source_name)
    assert len(scheduler.vocab_for_next_round_df) == 1
    # Check that the word was added to the table
    assert "飲む" in scheduler.vocab_for_next_round_df[TOKEN_COLNAME].tolist()


def test_add_voc_with_unknown_kanjis(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    min_days_btwn_kanji_and_voc = 3
    scheduler = booktocards.scheduler.Scheduler(
        kb=kb,
        n_days_study=2,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    # Prepare doc
    doc = "食べる飲む歌う。歌う。感じる。笑う。寝る。"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    # Try to put as "for next round" a word for which kanji isn't clearly known
    with pytest.raises(KanjiNotKnownError):
        scheduler.add_vocab_for_next_round(token="飲む", source_name=source_name)
    # Add as "of interest" vocab with unknown kanji
    scheduler.add_vocab_of_interest(token="歌う", source_name=source_name)
    # Is added to vocab_w_uncertain_status_df?
    assert (
        "歌う" in scheduler.vocab_w_uncertain_status_df[TOKEN_COLNAME].tolist()
    )
    # Only once?
    assert len(scheduler.vocab_w_uncertain_status_df) == 1
    # Check cannot add for round after next in current state
    with pytest.raises(KanjiNotKnownOrAddedError):
        scheduler.add_vocab_for_rounds_after_next(
            token="歌う", source_name=source_name
        )
    # Add kanji to next round, and check when now can
    scheduler.add_kanji_for_next_round(kanji="歌", source_name=source_name)
    with pytest.raises(KanjiNotKnownError):
        scheduler.add_vocab_for_next_round(token="歌う", source_name=source_name)
    scheduler.add_vocab_for_rounds_after_next(
        token="歌う", source_name=source_name
    )
    # Check kanji was added to kanji_for_next_round
    assert "歌" in scheduler.kanji_for_next_round_df[KANJI_COLNAME].tolist()
    # Check that token was removed from uncertain table
    assert (
        "歌う"
        not in scheduler.vocab_w_uncertain_status_df[TOKEN_COLNAME].tolist()
    )
    # Check that added to vocab for rounds after next
    assert (
        "歌う"
        in scheduler.vocab_for_rounds_after_next_df[TOKEN_COLNAME].tolist()
    )
    # Check the due date for the vocab
    vocab_for_rounds_after_next_df = scheduler.vocab_for_rounds_after_next_df
    token_df = vocab_for_rounds_after_next_df[
        (vocab_for_rounds_after_next_df[TOKEN_COLNAME] == "歌う")
        & (vocab_for_rounds_after_next_df[SOURCE_NAME_COLNAME] == source_name)
    ]
    assert len(token_df) == 1
    assert token_df.loc[
        token_df.index[0], TO_BE_STUDIED_FROM_DATE_COLNAME
    ] == (scheduler.today + timedelta(min_days_btwn_kanji_and_voc))


def test_add_voc_with_kanji_set_to_add_to_known(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    min_days_btwn_kanji_and_voc = 3
    scheduler = booktocards.scheduler.Scheduler(
        kb=kb,
        n_days_study=2,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    # Try to put as "of interest" a voc that's already marked as known
    doc = "食べる飲む歌う。歌う。"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    # Put as "for next round" a word for which kanji was just set to be added
    # as known
    scheduler.set_kanji_to_add_to_known(kanji="飲")
    scheduler.add_vocab_for_next_round(token="飲む", source_name=source_name)
    assert len(scheduler.vocab_for_next_round_df) == 1
    # Check that the word was added to the table
    assert "飲む" in scheduler.vocab_for_next_round_df[TOKEN_COLNAME].tolist()


def test_add_to_much_voc_complains(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc = "食べる飲む歌う。感じる。"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    min_days_btwn_kanji_and_voc = 3
    # Add too much voc to vocab of interest
    scheduler = booktocards.scheduler.Scheduler(
        kb=kb,
        n_days_study=1,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    scheduler.set_kanji_to_add_to_known(kanji="食")
    scheduler.set_kanji_to_add_to_known(kanji="飲")
    scheduler.add_vocab_for_next_round(token="食べる", source_name=source_name)
    scheduler.add_kanji_for_next_round(kanji="飲", source_name=source_name)
    with pytest.raises(EnoughItemsAddedError):
        scheduler.set_kanji_to_add_to_known(kanji="感")
        scheduler.add_vocab_for_next_round(
            token="感じる", source_name=source_name
        )
    # Try adding a kanji
    with pytest.raises(EnoughItemsAddedError):
        scheduler.add_kanji_for_next_round(kanji="歌", source_name=source_name)


def test_get_studiable_voc_1_doc(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    min_days_btwn_kanji_and_voc = 3
    scheduler = booktocards.scheduler.Scheduler(
        kb=kb,
        n_days_study=2,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    # Prepare doc
    doc = "食べる飲む歌う。歌う。感じる。笑う。寝る。"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    # Get all voc as studiable
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 6
    # Add one to known
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 5
    # Add one to of interest
    scheduler.add_vocab_of_interest(token="飲む", source_name=source_name)
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 4
    # Add one to next round from of interest
    scheduler.set_kanji_to_add_to_known(
        kanji="飲",
    )
    scheduler.add_vocab_for_next_round(token="飲む", source_name=source_name)
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 4
    # Add one for round after next
    scheduler.add_vocab_of_interest(token="感じる", source_name=source_name)
    scheduler.add_kanji_for_next_round(kanji="感", source_name=source_name)
    scheduler.add_vocab_for_rounds_after_next(
        token="感じる", source_name=source_name
    )
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 3
    # Add one to be set to known
    scheduler.set_vocab_to_add_to_known(token="歌う")
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 2
    # Add one to be set to suspended
    scheduler.set_vocab_to_add_to_suspended(
        token="笑う", source_name=source_name
    )
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 1


def test_get_studiable_voc_2_docs(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    min_days_btwn_kanji_and_voc = 3
    scheduler = booktocards.scheduler.Scheduler(
        kb=kb,
        n_days_study=2,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    # Add doc 1
    doc1 = "食べる飲む歌う。歌う。感じる。笑う。寝る。"
    source_name1 = "test_doc1"
    kb.add_doc(doc=doc1, doc_name=source_name1, drop_ascii_alphanum_toks=False)
    # Add doc 2
    doc2 = "眠る？起きる？食べる。"
    source_name2 = "test_doc2"
    kb.add_doc(doc=doc2, doc_name=source_name2, drop_ascii_alphanum_toks=False)
    # Get all voc as studiable
    studiable_voc_df = scheduler.get_studiable_voc()
    print(studiable_voc_df)
    assert len(studiable_voc_df) == 9
    # Get studiable voc from doc1
    studiable_voc_df = scheduler.get_studiable_voc(source_name=source_name1)
    assert len(studiable_voc_df) == 6
    # Get studiable voc from doc2
    studiable_voc_df = scheduler.get_studiable_voc(source_name=source_name2)
    assert len(studiable_voc_df) == 3


def test_get_studiable_kanji(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    min_days_btwn_kanji_and_voc = 3
    scheduler = booktocards.scheduler.Scheduler(
        kb=kb,
        n_days_study=2,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    # Prepare doc
    doc = "食べる飲む歌う。歌う。感じる。笑う。寝る。"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    # Get all kanji as studiable
    studiable_kanji_df = scheduler.get_studiable_kanji()
    assert len(studiable_kanji_df) == 6
    # Add one to known
    kb.set_item_to_known(
        item_value="食",
        item_colname=KANJI_COLNAME,
        table_name=KANJI_TABLE_NAME,
    )
    studiable_kanji_df = scheduler.get_studiable_kanji()
    assert len(studiable_kanji_df) == 5
    # Add one to next round
    scheduler.add_kanji_for_next_round(kanji="飲", source_name=source_name)
    studiable_kanji_df = scheduler.get_studiable_kanji()
    assert len(studiable_kanji_df) == 4
    # Add one to be set to known
    scheduler.set_kanji_to_add_to_known(kanji="歌")
    studiable_kanji_df = scheduler.get_studiable_kanji()
    assert len(studiable_kanji_df) == 3
    # Add one to be set to supsended
    scheduler.set_kanji_to_add_to_suspended(kanji="感", source_name=source_name)
    studiable_kanji_df = scheduler.get_studiable_kanji()
    assert len(studiable_kanji_df) == 2


class MockScheduler(booktocards.scheduler.Scheduler):
    """Scheduler with updated make_*_cards methods

    make_voc_cards_from_df and make_kanji_cards_from_df always return
    self.voc_cards and self.kanji_cards respectively
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.voc_cards = pd.DataFrame(
            {"tok": ["tok1", "tok2"], "source": ["source1", "source2"]}
        )
        self.kanji_cards = pd.DataFrame(
            {"kanji": ["kanji1", "kanji2"], "source": ["source1", "source2"]}
        )

    def make_voc_cards_from_df(self, *args, **kwargs):
        return self.voc_cards

    def make_kanji_cards_from_df(self, *args, **kwargs):
        return self.kanji_cards


def test_end_scheduling(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    min_days_btwn_kanji_and_voc = 3
    scheduler = MockScheduler(
        kb=kb,
        n_days_study=2,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    # Add doc
    doc = "食べる飲む歌う。歌う。感じる。笑う。寝る。"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    # Set 食 as to add to known
    scheduler.set_kanji_to_add_to_known(kanji="食")
    # Add 食べる for next round
    scheduler.add_vocab_of_interest(
        token="食べる",
        source_name=source_name,
    )
    # Add 飲 to next round
    scheduler.add_vocab_of_interest(token="飲む", source_name=source_name)
    scheduler.add_kanji_for_next_round(kanji="飲", source_name=source_name)
    # Add 飲む to round after next
    scheduler.add_vocab_for_rounds_after_next(
        token="飲む", source_name=source_name
    )
    # Add 歌う to vocab of interest and leave it in limbo
    scheduler.add_vocab_of_interest(token="歌う", source_name=source_name)
    # Add 感 to add to suspended
    scheduler.set_kanji_to_add_to_suspended(kanji="感", source_name=source_name)
    # Add 感じる to add to known
    scheduler.set_vocab_to_add_to_known(token="感じる")
    # Add 笑う to add tu suspended
    scheduler.set_vocab_to_add_to_suspended(
        token="笑う", source_name=source_name
    )
    # End the scheduling
    sched_out = scheduler.end_scheduling(
        translate_source_ex=True,
        sanseido_manipulator=None,
        tatoeba_db=None,
        deepl_translator=None,
        for_anki=False,
    )
    # Check the output
    vocab_df = pd.read_csv(filepath_or_buffer=sched_out["vocab"])
    kanji_df = pd.read_csv(filepath_or_buffer=sched_out["kanji"])
    pd.testing.assert_frame_equal(kanji_df, scheduler.kanji_cards)
    # Reload the kb
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    token_df = kb[TOKEN_TABLE_NAME]
    kanji_df = kb[KANJI_TABLE_NAME]
    # Check 食 as to add to known
    assert (
        kanji_df.loc[kanji_df[KANJI_COLNAME] == "食", IS_KNOWN_COLNAME]
    ).all()
    # Check 食べる for next round
    assert (
        token_df.loc[
            token_df[TOKEN_COLNAME] == "食べる", IS_ADDED_TO_ANKI_COLNAME
        ]
    ).all()
    # Check 飲 to next round
    assert (
        kanji_df.loc[kanji_df[KANJI_COLNAME] == "飲", IS_ADDED_TO_ANKI_COLNAME]
    ).all()
    # Check 飲む to round after next
    assert not (
        token_df.loc[token_df[TOKEN_COLNAME] == "飲む", IS_ADDED_TO_ANKI_COLNAME]
    ).all()
    assert (
        not (
            token_df.loc[
                token_df[TOKEN_COLNAME] == "飲む",
                TO_BE_STUDIED_FROM_DATE_COLNAME,
            ]
        )
        .isnull()
        .any()
    )
    # Check 歌う is nowhere
    assert not (
        token_df.loc[token_df[TOKEN_COLNAME] == "歌う", IS_ADDED_TO_ANKI_COLNAME]
    ).all()
    # Check 感 to add to suspended
    assert (
        kanji_df.loc[
            kanji_df[KANJI_COLNAME] == "感", IS_SUPSENDED_FOR_SOURCE_COLNAME
        ]
    ).all()
    # Check 感じる to add to known
    assert not (
        token_df.loc[
            token_df[TOKEN_COLNAME] == "感じる", IS_ADDED_TO_ANKI_COLNAME
        ]
    ).any()
    assert (
        token_df.loc[token_df[TOKEN_COLNAME] == "感じる", IS_KNOWN_COLNAME]
    ).all()
    # Check 笑う to add tu suspended
    assert (
        token_df.loc[
            token_df[IS_SUPSENDED_FOR_SOURCE_COLNAME] == "笑う",
            IS_KNOWN_COLNAME,
        ]
    ).all()

"""
Test level: integration (requires db, sanseido, jj to work well)
"""

import os
from datetime import timedelta

import pandas as pd
import pytest

import booktocards.kb
import booktocards.scheduler
from booktocards.kb import ColumnName, TableName
from booktocards.scheduler import (
    EnoughItemsAddedError,
    KanjiNotKnownError,
    KanjiNotKnownOrAddedError,
    NoAddableEntryError,
)


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
    kb.create_source_entry(source_name=source_name)
    kb.add_doc_from_full_text(
        doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False
    )
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=ColumnName.TOKEN,
        table_name=TableName.TOKENS,
    )
    with pytest.raises(NoAddableEntryError):
        scheduler.add_vocab_of_interest(token="食べる", source_name=source_name)
    # Put as "for next round" a word for which kanji is clearly known
    kb.set_item_to_known(
        item_value="飲", item_colname=ColumnName.KANJI, table_name=TableName.KANJIS
    )
    scheduler.add_vocab_for_next_round(token="飲む", source_name=source_name)
    assert len(scheduler.vocab_for_next_round_df) == 1
    # Check that the word was added to the table
    assert "飲む" in scheduler.vocab_for_next_round_df[ColumnName.TOKEN].tolist()


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
    kb.create_source_entry(source_name=source_name)
    kb.add_doc_from_full_text(
        doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False
    )
    # Try to put as "for next round" a word for which kanji isn't clearly known
    with pytest.raises(KanjiNotKnownError):
        scheduler.add_vocab_for_next_round(token="飲む", source_name=source_name)
    # Add as "of interest" vocab with unknown kanji
    scheduler.add_vocab_of_interest(token="歌う", source_name=source_name)
    # Is added to vocab_w_uncertain_status_df?
    assert "歌う" in scheduler.vocab_w_uncertain_status_df[ColumnName.TOKEN].tolist()
    # Only once?
    assert len(scheduler.vocab_w_uncertain_status_df) == 1
    # Check cannot add for round after next in current state
    with pytest.raises(KanjiNotKnownOrAddedError):
        scheduler.add_vocab_for_rounds_after_next(token="歌う", source_name=source_name)
    # Add kanji to next round, and check when now can
    scheduler.add_kanji_for_next_round(kanji="歌", source_name=source_name)
    with pytest.raises(KanjiNotKnownError):
        scheduler.add_vocab_for_next_round(token="歌う", source_name=source_name)
    scheduler.add_vocab_for_rounds_after_next(token="歌う", source_name=source_name)
    # Check kanji was added to kanji_for_next_round
    assert "歌" in scheduler.kanji_for_next_round_df[ColumnName.KANJI].tolist()
    # Check that token was removed from uncertain table
    assert (
        "歌う" not in scheduler.vocab_w_uncertain_status_df[ColumnName.TOKEN].tolist()
    )
    # Check that added to vocab for rounds after next
    assert "歌う" in scheduler.vocab_for_rounds_after_next_df[ColumnName.TOKEN].tolist()
    # Check the due date for the vocab
    vocab_for_rounds_after_next_df = scheduler.vocab_for_rounds_after_next_df
    token_df = vocab_for_rounds_after_next_df[
        (vocab_for_rounds_after_next_df[ColumnName.TOKEN] == "歌う")
        & (vocab_for_rounds_after_next_df[ColumnName.SOURCE_NAME] == source_name)
    ]
    assert len(token_df) == 1
    assert token_df.loc[token_df.index[0], ColumnName.TO_BE_STUDIED_FROM] == (
        scheduler.today + timedelta(min_days_btwn_kanji_and_voc)
    )


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
    kb.create_source_entry(source_name=source_name)
    kb.add_doc_from_full_text(
        doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False
    )
    # Put as "for next round" a word for which kanji was just set to be added
    # as known
    scheduler.set_kanji_to_add_to_known(kanji="飲")
    scheduler.add_vocab_for_next_round(token="飲む", source_name=source_name)
    assert len(scheduler.vocab_for_next_round_df) == 1
    # Check that the word was added to the table
    assert "飲む" in scheduler.vocab_for_next_round_df[ColumnName.TOKEN].tolist()


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
    kb.create_source_entry(source_name=source_name)
    kb.add_doc_from_full_text(
        doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False
    )
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
        scheduler.add_vocab_for_next_round(token="感じる", source_name=source_name)
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
    kb.create_source_entry(source_name=source_name)
    kb.add_doc_from_full_text(
        doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False
    )
    # Get all voc as studiable
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 6
    assert studiable_voc_df[ColumnName.TOKEN].iloc[0] != "歌う"
    # Get studiable voc sorted by count
    studiable_voc_df = scheduler.get_studiable_voc(sort_count=True)
    assert len(studiable_voc_df) == 6
    assert studiable_voc_df[ColumnName.TOKEN].iloc[0] == "歌う"
    # Get studiable voc with min_count
    studiable_voc_df = scheduler.get_studiable_voc(min_count=2)
    assert len(studiable_voc_df) == 1
    # NOTE: sort_seq_id will be tested in test_get_studiable_voc_2_docs
    # Add one to known
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=ColumnName.TOKEN,
        table_name=TableName.TOKENS,
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
    scheduler.add_vocab_for_rounds_after_next(token="感じる", source_name=source_name)
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 3
    # Add one to be set to known
    scheduler.set_vocab_to_add_to_known(token="歌う")
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 2
    # Add one to be set to suspended
    scheduler.set_vocab_to_add_to_suspended(token="笑う", source_name=source_name)
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 1
    # Get studiable voc with priority 1 (1 voc)
    studiable_voc_df = scheduler.get_studiable_voc(priority=1)
    assert len(studiable_voc_df) == 1
    # Get studiable voc with priority 2 (0 voc)
    studiable_voc_df = scheduler.get_studiable_voc(priority=2)
    assert len(studiable_voc_df) == 0


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
    kb.create_source_entry(source_name=source_name1)
    kb.add_doc_from_full_text(
        doc=doc1, doc_name=source_name1, drop_ascii_alphanum_toks=False
    )
    # Add doc 2
    doc2 = "眠る？起きる？食べる。"
    source_name2 = "test_doc2"
    kb.create_source_entry(source_name=source_name2)
    kb.add_doc_from_full_text(
        doc=doc2, doc_name=source_name2, drop_ascii_alphanum_toks=False
    )
    # Get all voc as studiable
    studiable_voc_df = scheduler.get_studiable_voc()
    assert len(studiable_voc_df) == 9
    # Assert that sorting by count/seq_id works
    studiable_voc_df = scheduler.get_studiable_voc()
    assert studiable_voc_df[ColumnName.TOKEN].iloc[0] == "食べる"
    assert studiable_voc_df[ColumnName.TOKEN].iloc[-1] == "食べる"
    studiable_voc_df = scheduler.get_studiable_voc(sort_seq_id=True)
    assert studiable_voc_df[ColumnName.TOKEN].iloc[0] == "食べる"
    assert studiable_voc_df[ColumnName.TOKEN].iloc[-1] == "寝る"
    studiable_voc_df = scheduler.get_studiable_voc(sort_count=True)
    assert studiable_voc_df[ColumnName.TOKEN].iloc[0] == "歌う"
    assert studiable_voc_df[ColumnName.TOKEN].iloc[-1] == "食べる"
    studiable_voc_df = scheduler.get_studiable_voc(sort_seq_id=True, sort_count=True)
    assert studiable_voc_df[ColumnName.TOKEN].iloc[0] == "歌う"
    assert studiable_voc_df[ColumnName.TOKEN].iloc[-1] == "寝る"
    # Get studiable voc from doc1
    studiable_voc_df = scheduler.get_studiable_voc(source_name=source_name1)
    assert len(studiable_voc_df) == 6
    # Get studiable voc from doc2
    studiable_voc_df = scheduler.get_studiable_voc(source_name=source_name2)
    assert len(studiable_voc_df) == 3


@pytest.mark.parametrize(
    "with_sequence",
    [True, False],
)
def test_get_studiable_voc_for_token_added_with_sequence(
    with_sequence: bool, monkeypatch, tmp_path
):
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
    # Prepare token
    token = "食べる"
    if with_sequence:
        sequence = "食べる飲む歌う。"
    else:
        sequence = None
    source_name = "test_doc"
    kb.create_source_entry(source_name=source_name)
    kb.add_token_with_sequence_to_doc(
        token=token,
        sequence=sequence,
        doc_name=source_name,
    )
    # Retrieve studiable voc with priority 2 (i.e. the token)
    studiable_voc_df = scheduler.get_studiable_voc(priority=2, min_count=0)
    assert len(studiable_voc_df) == 1
    assert studiable_voc_df[ColumnName.TOKEN].iloc[0] == token
    # Retrieve studiable voc with priority 1 (i.e. none)
    studiable_voc_df = scheduler.get_studiable_voc(priority=1, min_count=0)
    assert len(studiable_voc_df) == 0


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
    kb.create_source_entry(source_name=source_name)
    kb.add_doc_from_full_text(
        doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False
    )
    # Get all kanji as studiable
    studiable_kanji_df = scheduler.get_studiable_kanji()
    assert len(studiable_kanji_df) == 6
    # Add one to known
    kb.set_item_to_known(
        item_value="食",
        item_colname=ColumnName.KANJI,
        table_name=TableName.KANJIS,
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
    kb.create_source_entry(source_name=source_name)
    kb.add_doc_from_full_text(
        doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False
    )
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
    scheduler.add_vocab_for_rounds_after_next(token="飲む", source_name=source_name)
    # Add 歌う to vocab of interest and leave it in limbo
    scheduler.add_vocab_of_interest(token="歌う", source_name=source_name)
    # Add 感 to add to suspended
    scheduler.set_kanji_to_add_to_suspended(kanji="感", source_name=source_name)
    # Add 感じる to add to known
    scheduler.set_vocab_to_add_to_known(token="感じる")
    # Add 笑う to add tu suspended
    scheduler.set_vocab_to_add_to_suspended(token="笑う", source_name=source_name)
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
    pd.testing.assert_frame_equal(vocab_df, scheduler.voc_cards)
    pd.testing.assert_frame_equal(kanji_df, scheduler.kanji_cards)
    # Reload the kb
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    token_df = kb[TableName.TOKENS]
    kanji_df = kb[TableName.KANJIS]
    # Check 食 as to add to known
    assert (kanji_df.loc[kanji_df[ColumnName.KANJI] == "食", ColumnName.IS_KNOWN]).all()
    # Check 食べる for next round
    assert (
        token_df.loc[
            token_df[ColumnName.TOKEN] == "食べる", ColumnName.IS_ADDED_TO_ANKI
        ]
    ).all()
    # Check 飲 to next round
    assert (
        kanji_df.loc[kanji_df[ColumnName.KANJI] == "飲", ColumnName.IS_ADDED_TO_ANKI]
    ).all()
    # Check 飲む to round after next
    assert not (
        token_df.loc[token_df[ColumnName.TOKEN] == "飲む", ColumnName.IS_ADDED_TO_ANKI]
    ).all()
    assert (
        not (
            token_df.loc[
                token_df[ColumnName.TOKEN] == "飲む", ColumnName.TO_BE_STUDIED_FROM
            ]
        )
        .isnull()
        .any()
    )
    # Check 歌う is nowhere
    assert not (
        token_df.loc[token_df[ColumnName.TOKEN] == "歌う", ColumnName.IS_ADDED_TO_ANKI]
    ).all()
    # Check 感 to add to suspended
    assert (
        kanji_df.loc[
            kanji_df[ColumnName.KANJI] == "感", ColumnName.IS_SUSPENDED_FOR_SOURCE
        ]
    ).all()
    # Check 感じる to add to known
    assert not (
        token_df.loc[
            token_df[ColumnName.TOKEN] == "感じる", ColumnName.IS_ADDED_TO_ANKI
        ]
    ).any()
    assert (
        token_df.loc[token_df[ColumnName.TOKEN] == "感じる", ColumnName.IS_KNOWN]
    ).all()
    # Check 笑う to add tu suspended
    assert (
        token_df.loc[
            token_df[ColumnName.IS_SUSPENDED_FOR_SOURCE] == "笑う", ColumnName.IS_KNOWN
        ]
    ).all()

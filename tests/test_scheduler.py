"""
Test level: integration (requires db, sanseido, jj to work well)
"""
import os
import pytest
from unittest.mock import Mock
from datetime import timedelta

from booktocards.kb import (
    TOKEN_TABLE_NAME,
    KANJI_TABLE_NAME,
    TOKEN_COLNAME,
    KANJI_COLNAME,
    TO_BE_STUDIED_FROM_DATE_COLNAME,
    IS_KNOWN_COLNAME,
    IS_ADDED_TO_ANKI_COLNAME,
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
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase()
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
        item_value="飲", item_colname=KANJI_COLNAME, table_name=KANJI_TABLE_NAME
    )
    scheduler.add_vocab_for_next_round(token="飲む", source_name=source_name)
    assert len(scheduler.vocab_for_next_round_df) == 1
    # Check that the word was added to the table
    assert (
        "飲む" in scheduler.vocab_for_next_round_df[TOKEN_COLNAME].tolist()
    # Check that the status has been changed in kb
    )
    token_df = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        item_value="飲む",
        item_colname=TOKEN_COLNAME,
        source_name=source_name,
        only_not_added=False,
        only_not_known=False,
        only_not_suspended=True,
        max_study_date=None,
    )
    assert len(token_df) == 1
    assert token_df.loc[
        token_df.index[0], IS_ADDED_TO_ANKI_COLNAME
    ] == True


def test_add_voc_with_unknown_kanjis(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb and scheduler
    kb = booktocards.kb.KnowledgeBase()
    min_days_btwn_kanji_and_voc = 3
    scheduler = booktocards.scheduler.Scheduler(
        kb=kb,
        n_days_study=2,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    # Prepare doc
    doc = "食べる飲む歌う。歌う。"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
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
    # Still cannot end scheduler?
    with pytest.raises(UncertainVocRemainError):
        scheduler.end_scheduling(
            translate_source_ex=False,
            sanseido_manipulator=None,
            tatoeba_db=None,
            deepl_translator=None,
        )
    # Check cannot add for round after next in current state
    with pytest.raises(KanjiNotKnownOrAddedError):
        scheduler.add_vocab_for_rounds_after_next(
            token="歌う", source_name=source_name
        )
    # Add kanji to next round, and check when now can
    scheduler.add_kanji_for_next_round(kanji="歌", source_name=source_name)
    with pytest.raises(KanjiNotKnownError):
        scheduler.add_vocab_for_next_round(
            token="歌う", source_name=source_name
        )
    scheduler.add_vocab_for_rounds_after_next(
        token="歌う", source_name=source_name
    )
    # Check kanji was added to kanji_for_next_round
    assert "歌" in scheduler.kanji_for_next_round_df[KANJI_COLNAME].tolist()
    # Check the due date for the vocab
    token_df = scheduler.kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        item_value="歌う",
        item_colname=TOKEN_COLNAME,
        source_name=source_name,
        only_not_added=True,
        only_not_known=True,
        only_not_suspended=True,
        max_study_date=None,
    )
    assert len(token_df) == 1
    assert token_df.loc[
        token_df.index[0], TO_BE_STUDIED_FROM_DATE_COLNAME
    ] == (scheduler.today + timedelta(min_days_btwn_kanji_and_voc))
    # Check that token was removed from uncertain table
    assert (
        "歌う"
        not in scheduler.vocab_w_uncertain_status_df[TOKEN_COLNAME].tolist()
    )
    # Check that added to vocab for rounds after next
    assert (
        "歌う" in scheduler.vocab_for_rounds_after_next_df[TOKEN_COLNAME].tolist()
    )


def test_add_to_much_voc_complains(monkeypatch, tmp_path):
    # Handle temporary folders
    path = tmp_path.resolve()
    path_cards = os.path.join(path, "cards")
    path_kb = os.path.join(path, "kb")
    os.mkdir(path_cards)
    os.mkdir(path_kb)
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path_kb)
    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
    # Init kb
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる飲む歌う"
    source_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
    min_days_btwn_kanji_and_voc = 3
    # Add too much voc to vocab of interest
    scheduler = booktocards.scheduler.Scheduler(kb=kb,
        n_days_study=1,
        n_cards_days=2,
        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
    )
    scheduler.add_vocab_of_interest(token="食べる", source_name=source_name)
    scheduler.add_vocab_of_interest(token="飲む", source_name=source_name)
    with pytest.raises(EnoughItemsAddedError):
        scheduler.add_vocab_of_interest(token="歌う", source_name=source_name)
    # Try adding a kanji
    with pytest.raises(EnoughItemsAddedError):
        scheduler.add_kanji_for_next_round(kanji="歌", source_name=source_name)
    # Mark kanji from vocab_w_uncertain_status_df as known, and add to next round
    kb.set_item_to_known(item_value="食", item_colname=KANJI_COLNAME,
            table_name=KANJI_TABLE_NAME)
    scheduler.add_vocab_for_next_round(token="食べる", source_name=source_name)
    # Incidental: check vocab was removed from unertain df
    assert (
        "食べる"
        not in scheduler.vocab_w_uncertain_status_df[TOKEN_COLNAME].tolist()
    )
    # Mark other kanji as known, and try to add to next round
    scheduler.kb.set_item_to_known(item_value="歌", item_colname=KANJI_COLNAME,
            table_name=KANJI_TABLE_NAME)
    with pytest.raises(EnoughItemsAddedError):
        scheduler.add_vocab_for_next_round(
            token="歌う",
            source_name=source_name,
        )


#def test_make_cards(monkeypatch, tmp_path):
#    # Handle temporary folders
#    path = tmp_path.resolve()
#    path_cards = os.path.join(path, "cards")
#    path_kb = os.path.join(path, "kb")
#    os.mkdir(path_cards)
#    os.mkdir(path_kb)
#    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path_kb)
#    monkeypatch.setattr(booktocards.scheduler, "_cards_dirpath", path_cards)
#    # Init kb and scheduler
#    kb = booktocards.kb.KnowledgeBase()
#    min_days_btwn_kanji_and_voc = 3
#    scheduler = booktocards.scheduler.Scheduler(
#        kb=kb,
#        n_days_study=2,
#        n_cards_days=2,
#        min_days_btwn_kanji_and_voc=min_days_btwn_kanji_and_voc,
#    )
#    # Try to put as "of interest" a voc that's already marked as known
#    doc = "食べる飲む"
#    source_name = "test_doc"
#    kb.add_doc(doc=doc, doc_name=source_name, drop_ascii_alphanum_toks=False)
#    kb.set_item_to_known(
#        item_value="食べる",
#        item_colname=TOKEN_COLNAME,
#        table_name=TOKEN_TABLE_NAME,
#    )
#    # Add one voc, one kanji
#    kb.set_item_to_known(
#        item_value="飲", item_colname=KANJI_COLNAME, table_name=KANJI_TABLE_NAME
#    )
#    scheduler.add_vocab_for_next_round(token="飲む",
#            source_name=source_name)
#    scheduler.add_kanji_for_next_round(kanji="食", source_name=source_name)
#    # Make mocks for sanseido and tatoeba manipulators
#    sanseido_manipulator = Mock()
#    sanseido_manipulator.sanseido_dict.__contains__.return_value = True
#    sanseido_manipulator.sanseido_dict.__getitem__.return_value = "mock_sanseido_def"
#    # TODO: do the same with tatoeba... which is more complicated
#    # test
#    card_filepaths = scheduler.end_scheduling(
#        translate_source_ex=False,
#        sanseido_manipulator=None,
#        tatoeba_db=None,
#        deepl_translator=None,
#    )
#

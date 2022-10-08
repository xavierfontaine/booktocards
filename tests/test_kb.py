import datetime
import pandas as pd
import pytest
import os

import booktocards.kb
from booktocards.kb import (
    DATA_MODEL,
    TOKEN_TABLE_NAME,
    KANJI_TABLE_NAME,
    SEQ_TABLE_NAME,
    TOKEN_COLNAME,
    KANJI_COLNAME,
    IS_KNOWN_COLNAME,
    IS_ADDED_TO_ANKI_COLNAME,
    IS_SUPSENDED_FOR_SOURCE_COLNAME,
    TO_BE_STUDIED_FROM_DATE_COLNAME,
    ASSOCIATED_TOKS_FROM_SOURCE_COLNAME,
)

# Common to all tests
exp_self_tables = [TOKEN_TABLE_NAME, KANJI_TABLE_NAME, SEQ_TABLE_NAME]
exp_n_tables = len(exp_self_tables)


def test_files_are_created_and_reloaded(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # First, make sure that the list of tables to be tested is consitent with
    # the data model
    assert set(exp_self_tables) == set(list(DATA_MODEL.keys()))
    # Check there is not file
    assert len(os.listdir(path)) == 0
    # Check 1 file has been created
    kb = booktocards.kb.KnowledgeBase()
    assert len(os.listdir(path)) == 1
    # Check that tables are empty df
    for table_name in exp_self_tables:
        assert isinstance(kb.__dict__[table_name], pd.DataFrame)
        assert kb.__dict__[table_name].shape[0] == 0
    # Add a doc and check everything has been added
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    assert kb.__dict__[TOKEN_TABLE_NAME].shape[0] == 2
    assert "食べる" in kb.__dict__[TOKEN_TABLE_NAME][TOKEN_COLNAME].to_list()
    assert "吐く" in kb.__dict__[TOKEN_TABLE_NAME][TOKEN_COLNAME].to_list()
    assert kb.__dict__[KANJI_TABLE_NAME].shape[0] == 2
    assert "食" in kb.__dict__[KANJI_TABLE_NAME][KANJI_COLNAME].to_list()
    assert sorted(
        kb.__dict__[KANJI_TABLE_NAME][
            ASSOCIATED_TOKS_FROM_SOURCE_COLNAME
        ].to_list()
    ) == sorted([["食べる"], ["吐く"]])
    assert kb.__dict__[SEQ_TABLE_NAME].shape[0] == 1
    # Reload and check everything is still here
    kb = booktocards.kb.KnowledgeBase()
    assert kb.__dict__[TOKEN_TABLE_NAME].shape[0] == 2
    assert kb.__dict__[KANJI_TABLE_NAME].shape[0] == 2
    assert kb.__dict__[SEQ_TABLE_NAME].shape[0] == 1


def test_error_when_adding_again_a_doc(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Adding again should fail
    with pytest.raises(ValueError):
        kb.add_doc(
            doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False
        )


def test_remove_doc_works(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Remove the doc
    kb.remove_doc(doc_name="test_doc")
    for table_name in exp_self_tables:
        assert kb.__dict__[table_name].shape[0] == 0
    # Is the doc still absent reloading?
    kb = booktocards.kb.KnowledgeBase()
    for table_name in exp_self_tables:
        assert kb.__dict__[table_name].shape[0] == 0


def test_set_to_known_works(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc2", drop_ascii_alphanum_toks=False)
    # Make sure that IS_KNOWN_COLNAME is False
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME]) == 0
    # Check what happens if set to known
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME]) == 2
    # Check this is all well saved
    kb = booktocards.kb.KnowledgeBase()
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME]) == 2


def test_automatically_set_known_for_new_doc(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Set to known
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    # Should be one set to known
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME]) == 1
    # Add a 2nd doc (the same). Should be 2 set to known
    kb.add_doc(doc=doc, doc_name="test_doc2", drop_ascii_alphanum_toks=False)
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME]) == 2


def test_set_added_to_anki_works(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc2", drop_ascii_alphanum_toks=False)
    # Make sure that all is false
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_ADDED_TO_ANKI_COLNAME]) == 0
    # Check what happens if set to known
    kb.set_item_to_added_to_anki(
        item_value="食べる",
        source_name="test_doc2",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_ADDED_TO_ANKI_COLNAME]) == 1
    # Check this is all well saved
    kb = booktocards.kb.KnowledgeBase()
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_ADDED_TO_ANKI_COLNAME]) == 1


def test_set_is_suspended_for_source_works(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc2", drop_ascii_alphanum_toks=False)
    # Make sure that all is false
    assert (
        sum(kb.__dict__[TOKEN_TABLE_NAME][IS_SUPSENDED_FOR_SOURCE_COLNAME])
        == 0
    )
    # Check what happens if set to known
    kb.set_item_to_suspended_for_source(
        item_value="食べる",
        source_name="test_doc2",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    assert (
        sum(kb.__dict__[TOKEN_TABLE_NAME][IS_SUPSENDED_FOR_SOURCE_COLNAME])
        == 1
    )
    # Check this is all well saved
    kb = booktocards.kb.KnowledgeBase()
    assert (
        sum(kb.__dict__[TOKEN_TABLE_NAME][IS_SUPSENDED_FOR_SOURCE_COLNAME])
        == 1
    )


def test_set_study_from_date_for_token_source(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Initialize kb and add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる"
    doc_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=doc_name, drop_ascii_alphanum_toks=False)
    # Set date & check
    today = datetime.date.today()
    kb.set_study_from_date_for_token_source(
        token_value="食べる",
        source_name=doc_name,
        date=today,
    )
    assert kb[TOKEN_TABLE_NAME][TO_BE_STUDIED_FROM_DATE_COLNAME].tolist() == [
        today
    ]


def test_get_items_works(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Initialize kb and add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    doc_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=doc_name, drop_ascii_alphanum_toks=False)
    # Set 吐く as known
    kb.set_item_to_added_to_anki(
        item_value="吐く",
        source_name=doc_name,
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    # Set 食 as added to Anki
    kb.set_item_to_known(
        item_value="食",
        item_colname=KANJI_COLNAME,
        table_name=KANJI_TABLE_NAME,
    )
    # Set due date on 食べる
    today = datetime.date.today()
    kb.set_study_from_date_for_token_source(
        token_value="食べる",
        source_name=doc_name,
        date=today,
    )
    # Get all tokens
    exp_out = ["食べる", "吐く"]
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added_known_suspended=False,
        item_value=None,
        source_name=None,
        item_colname=None,
        max_study_date=None,
    )
    obs_out = items[TOKEN_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Get all token not added/known/suspended
    exp_out = ["食べる"]
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added_known_suspended=True,
        item_value=None,
        source_name=None,
        item_colname=None,
        max_study_date=None,
    )
    obs_out = items[TOKEN_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Get all kanji not added/known/suspended
    exp_out = ["吐"]
    items = kb.get_items(
        table_name=KANJI_TABLE_NAME,
        only_not_added_known_suspended=True,
        item_value=None,
        source_name=None,
        item_colname=None,
        max_study_date=None,
    )
    obs_out = items[KANJI_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Get specific token
    exp_out = ["食べる"]
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added_known_suspended=False,
        item_value=exp_out[0],
        source_name=doc_name,
        item_colname=TOKEN_COLNAME,
        max_study_date=None,
    )
    obs_out = items[TOKEN_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Get specific kanji
    exp_out = ["吐"]
    items = kb.get_items(
        table_name=KANJI_TABLE_NAME,
        only_not_added_known_suspended=False,
        item_value=exp_out[0],
        source_name=doc_name,
        item_colname=KANJI_COLNAME,
        max_study_date=None,
    )
    obs_out = items[KANJI_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Return df of size zero when no match
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added_known_suspended=False,
        item_value="鳥",
        source_name=doc_name,
        item_colname=TOKEN_COLNAME,
        max_study_date=None,
    )
    assert items.shape[0] == 0
    # Get back with specific date
    tomorrow = today + datetime.timedelta(days=1)
    exp_out = ["食べる"]
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added_known_suspended=False,
        item_value="食べる",
        source_name=doc_name,
        item_colname=TOKEN_COLNAME,
        max_study_date=tomorrow,
    )
    obs_out = items[TOKEN_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # No item when max_study_date is before date
    yesterday = today + datetime.timedelta(days=-1)
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added_known_suspended=False,
        item_value="食べる",
        source_name=doc_name,
        item_colname=TOKEN_COLNAME,
        max_study_date=yesterday,
    )
    assert items.shape[0] == 0

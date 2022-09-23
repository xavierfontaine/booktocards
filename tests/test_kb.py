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
    # Check 3 files are being created
    kb = booktocards.kb.KnowledgeBase()
    assert len(os.listdir(path)) == exp_n_tables
    # Check that tables are empty df
    for table_name in exp_self_tables:
        assert isinstance(kb.__dict__[table_name], pd.DataFrame)
        assert kb.__dict__[table_name].shape[0] == 0
    # Add a doc and check everything has been added
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc")
    assert kb.__dict__[TOKEN_TABLE_NAME].shape[0] == 2
    assert "食べる" in kb.__dict__[TOKEN_TABLE_NAME][TOKEN_COLNAME].to_list()
    assert "吐く" in kb.__dict__[TOKEN_TABLE_NAME][TOKEN_COLNAME].to_list()
    assert kb.__dict__[KANJI_TABLE_NAME].shape[0] == 2
    assert "食" in kb.__dict__[KANJI_TABLE_NAME][KANJI_COLNAME].to_list()
    assert set(kb.__dict__[KANJI_TABLE_NAME][
        ASSOCIATED_TOKS_FROM_SOURCE_COLNAME
    ].to_list()) == set([["食べる"], ["吐く"]])
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
    kb.add_doc(doc=doc, doc_name="test_doc")
    # Adding again should fail
    with pytest.raises(ValueError):
        kb.add_doc(doc=doc, doc_name="test_doc")


def test_remove_doc_works(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc")
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
    kb.add_doc(doc=doc, doc_name="test_doc")
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc2")
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


def test_automatically_set_know_for_new_doc(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc")
    # Set to known
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    # Should be one set to known
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME]) == 1
    # Add a 2nd doc (the same). Should be 2 set to known
    kb.add_doc(doc=doc, doc_name="test_doc2")
    assert sum(kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME]) == 2


def test_set_added_to_anki_works(monkeypatch, tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    monkeypatch.setattr(booktocards.kb, "_kb_dirpath", path)
    # Add doc
    kb = booktocards.kb.KnowledgeBase()
    doc = "食べる吐く"
    kb.add_doc(doc=doc, doc_name="test_doc")
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc2")
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
    kb.add_doc(doc=doc, doc_name="test_doc")
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc2")
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

import copy
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
    SEQ_COLNAME,
    SEQ_ID_COLNAME,
    IS_KNOWN_COLNAME,
    IS_ADDED_TO_ANKI_COLNAME,
    IS_SUPSENDED_FOR_SOURCE_COLNAME,
    TO_BE_STUDIED_FROM_DATE_COLNAME,
    ASSOCIATED_TOKS_FROM_SOURCE_COLNAME,
)

# Common to all tests
exp_self_tables = [TOKEN_TABLE_NAME, KANJI_TABLE_NAME, SEQ_TABLE_NAME]
exp_n_tables = len(exp_self_tables)


def test_files_are_created_and_reloaded(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # First, make sure that the list of tables to be tested is consitent with
    # the data model
    assert set(exp_self_tables) == set(list(DATA_MODEL.keys()))
    # Check there is not file
    assert len(os.listdir(path)) == 0
    # Check 3 files and one folder have been created
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    assert len(os.listdir(path)) == 4
    # Check that only one of these are a folder
    assert len(next(os.walk(path))[1]) == 1
    # Inside the folder, checjk 3 files have been created
    backup_dirname = next(os.walk(path))[1][0]
    backup_path = os.path.join(
        path,
        backup_dirname,
    )
    assert len(os.listdir(backup_path)) == 3
    # Check that tables are empty df
    for table_name in exp_self_tables:
        assert isinstance(kb.__dict__[table_name], pd.DataFrame)
        assert kb.__dict__[table_name].shape[0] == 0
    # Add a doc and check everything has been added
    doc = "食べる飲む"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    assert kb.__dict__[TOKEN_TABLE_NAME].shape[0] == 2
    assert "食べる" in kb.__dict__[TOKEN_TABLE_NAME][TOKEN_COLNAME].to_list()
    assert "飲む" in kb.__dict__[TOKEN_TABLE_NAME][TOKEN_COLNAME].to_list()
    assert kb.__dict__[KANJI_TABLE_NAME].shape[0] == 2
    assert "食" in kb.__dict__[KANJI_TABLE_NAME][KANJI_COLNAME].to_list()
    assert sorted(
        kb.__dict__[KANJI_TABLE_NAME][
            ASSOCIATED_TOKS_FROM_SOURCE_COLNAME
        ].to_list()
    ) == sorted([["食べる"], ["飲む"]])
    assert kb.__dict__[SEQ_TABLE_NAME].shape[0] == 1
    # Save, reload and check everything is still here
    kb.save_kb()
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    assert kb.__dict__[TOKEN_TABLE_NAME].shape[0] == 2
    assert kb.__dict__[KANJI_TABLE_NAME].shape[0] == 2
    assert kb.__dict__[SEQ_TABLE_NAME].shape[0] == 1


def test_adding_twice_same_doc_adds_items_once(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc = "食べる飲む"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    kb_dict_1st_add = copy.deepcopy(kb.__dict__)
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    kb_dict_2nd_add = copy.deepcopy(kb.__dict__)
    # Make sure that IS_KNOWN_COLNAME is False
    for table_name in [TOKEN_TABLE_NAME, KANJI_TABLE_NAME]:
        assert kb_dict_1st_add[table_name].equals(kb_dict_2nd_add[table_name])


def test_adding_entries_on_same_doc_works(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc1 = "食べる飲む"
    kb.add_doc(doc=doc1, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Add a 2nd doc
    doc2 = "眠る"
    kb.add_doc(doc=doc2, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Check all tokens, kanjis and seqs have been added
    assert set(kb.__dict__[TOKEN_TABLE_NAME][TOKEN_COLNAME].to_list()) == set(["食べる", "飲む", "眠る"])
    assert set(kb.__dict__[KANJI_TABLE_NAME][KANJI_COLNAME].to_list()) == set(["食", "飲", "眠"])
    assert kb.__dict__[SEQ_TABLE_NAME][SEQ_COLNAME].to_list() == [doc1, doc2]
    # Check seq ids make sense
    assert kb.__dict__[SEQ_TABLE_NAME][SEQ_ID_COLNAME].to_list() == [0, 1]


def test_remove_doc_works(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc = "食べる飲む"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Remove the doc
    kb.remove_doc(doc_name="test_doc")
    for table_name in exp_self_tables:
        assert kb.__dict__[table_name].shape[0] == 0
    # Is the doc still absent reloading?
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    for table_name in exp_self_tables:
        assert kb.__dict__[table_name].shape[0] == 0


def test_set_to_known_works(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc = "食べる飲む"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc2", drop_ascii_alphanum_toks=False)
    # Make sure that IS_KNOWN_COLNAME is False
    assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 0
    # Check what happens if set to known
    kb.set_item_to_known(
        item_value="食べる",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 2
    # Check this is still here after saving/reloading
    kb.save_kb()
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 2


@pytest.mark.parametrize(
    "set_voc_tag", ["is_known", "is_added_to_anki", "to_be_studied_from"]
)
def test_automatically_set_known_for_new_doc(tmp_path, set_voc_tag):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc = "食べる飲む"
    doc_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=doc_name, drop_ascii_alphanum_toks=False)
    # Set to known
    if set_voc_tag == "is_known":
        kb.set_item_to_known(
            item_value="食べる",
            item_colname=TOKEN_COLNAME,
            table_name=TOKEN_TABLE_NAME,
        )
    elif set_voc_tag == "is_added_to_anki":
        kb.set_item_to_added_to_anki(
            item_value="食べる",
            source_name=doc_name,
            item_colname=TOKEN_COLNAME,
            table_name=TOKEN_TABLE_NAME,
        )
    elif set_voc_tag == "to_be_studied_from":
        kb.set_study_from_date_for_token_source(
            token_value="食べる",
            source_name=doc_name,
            date=datetime.date.today(),
        )
    else:
        raise ValueError("Unexpected test parametrization")
    # Test current number of known
    if set_voc_tag == "is_known":
        assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 1
    else:
        assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 0
    # Add a 2nd doc (the same). Check number of known.
    kb.add_doc(doc=doc, doc_name="test_doc2", drop_ascii_alphanum_toks=False)
    if set_voc_tag == "is_known":
        assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 2
    else:
        assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 1
    # Add a 3nd doc (the same). Check number of known.
    kb.add_doc(doc=doc, doc_name="test_doc3", drop_ascii_alphanum_toks=False)
    if set_voc_tag == "is_known":
        assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 3
    else:
        assert kb.__dict__[TOKEN_TABLE_NAME][IS_KNOWN_COLNAME].sum() == 2


def test_set_added_to_anki_works(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc = "食べる飲む"
    kb.add_doc(doc=doc, doc_name="test_doc", drop_ascii_alphanum_toks=False)
    # Add a 2nd doc (the same)
    kb.add_doc(doc=doc, doc_name="test_doc2", drop_ascii_alphanum_toks=False)
    # Make sure that all is false
    assert kb.__dict__[TOKEN_TABLE_NAME][IS_ADDED_TO_ANKI_COLNAME].sum() == 0
    # Check what happens if set to known and added to Anki
    kb.set_item_to_added_to_anki(
        item_value="食べる",
        source_name="test_doc2",
        item_colname=TOKEN_COLNAME,
        table_name=TOKEN_TABLE_NAME,
    )
    assert kb.__dict__[TOKEN_TABLE_NAME][IS_ADDED_TO_ANKI_COLNAME].sum() == 1
    # Check this is still here after saving/reloading
    kb.save_kb()
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    assert kb.__dict__[TOKEN_TABLE_NAME][IS_ADDED_TO_ANKI_COLNAME].sum() == 1


def test_set_is_suspended_for_source_works(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc = "食べる飲む"
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
        kb.__dict__[TOKEN_TABLE_NAME][IS_SUPSENDED_FOR_SOURCE_COLNAME].sum()
        == 1
    )
    # Check this is still here after saving/reloading
    kb.save_kb()
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    assert (
        kb.__dict__[TOKEN_TABLE_NAME][IS_SUPSENDED_FOR_SOURCE_COLNAME].sum()
        == 1
    )


def test_set_study_from_date_for_token_source(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Initialize kb and add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
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


def test_get_items_works(tmp_path):
    # Change path where kb will be saved
    path = tmp_path.resolve()
    # Initialize kb and add doc
    kb = booktocards.kb.KnowledgeBase(kb_dirpath=path)
    doc = "食べる飲む"
    doc_name = "test_doc"
    kb.add_doc(doc=doc, doc_name=doc_name, drop_ascii_alphanum_toks=False)
    # Get all tokens
    exp_out = ["食べる", "飲む"]
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added=True,
        only_not_known=True,
        only_not_suspended=True,
        only_no_study_date=True,
        item_value=None,
        source_name=None,
        item_colname=None,
        max_study_date=None,
    )
    obs_out = items[TOKEN_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Set 飲む as known
    kb.set_item_to_added_to_anki(
        item_value="飲む",
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
    exp_out = ["食べる", "飲む"]
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added=False,
        only_not_known=False,
        only_not_suspended=False,
        only_no_study_date=False,
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
        only_not_added=True,
        only_not_known=True,
        only_not_suspended=True,
        only_no_study_date=False,
        item_value=None,
        source_name=None,
        item_colname=None,
        max_study_date=None,
    )
    obs_out = items[TOKEN_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Get all token without study date
    exp_out = ["飲む"]
    items = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added=False,
        only_not_known=False,
        only_not_suspended=False,
        only_no_study_date=True,
        item_value=None,
        source_name=None,
        item_colname=None,
        max_study_date=None,
    )
    obs_out = items[TOKEN_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Get all kanji not added/known/suspended
    exp_out = ["飲"]
    items = kb.get_items(
        table_name=KANJI_TABLE_NAME,
        only_not_added=True,
        only_not_known=True,
        only_not_suspended=True,
        only_no_study_date=True,
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
        only_not_added=False,
        only_not_known=False,
        only_not_suspended=False,
        only_no_study_date=False,
        item_value=exp_out[0],
        source_name=doc_name,
        item_colname=TOKEN_COLNAME,
        max_study_date=None,
    )
    obs_out = items[TOKEN_COLNAME].tolist()
    assert sorted(exp_out) == sorted(obs_out)
    # Get specific kanji
    exp_out = ["飲"]
    items = kb.get_items(
        table_name=KANJI_TABLE_NAME,
        only_not_added=False,
        only_not_known=False,
        only_not_suspended=False,
        only_no_study_date=False,
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
        only_not_added=False,
        only_not_known=False,
        only_not_suspended=False,
        only_no_study_date=False,
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
        only_not_added=False,
        only_not_known=False,
        only_not_suspended=False,
        only_no_study_date=False,
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
        only_not_added=False,
        only_not_known=False,
        only_not_suspended=False,
        only_no_study_date=False,
        item_value="食べる",
        source_name=doc_name,
        item_colname=TOKEN_COLNAME,
        max_study_date=yesterday,
    )
    assert items.shape[0] == 0

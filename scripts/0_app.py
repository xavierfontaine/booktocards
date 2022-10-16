import deepl
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from io import StringIO
from st_aggrid import AgGrid, GridOptionsBuilder, AgGridReturn
from typing import Literal, Union

from booktocards import io
from booktocards.annotations import Token, Kanji, SourceName
from booktocards.datacl import VocabCard, KanjiCard
from booktocards.kb import KnowledgeBase
from booktocards.jj_dicts import ManipulateSanseido
from booktocards.tatoeba import ManipulateTatoeba
from booktocards.text import get_unique_kanjis
from booktocards.kb import (
    TOKEN_TABLE_NAME,
    KANJI_TABLE_NAME,
    SEQ_TABLE_NAME,
    SOURCE_NAME_COLNAME,
    TOKEN_COLNAME,
    KANJI_COLNAME,
    SEQ_COLNAME,
    IS_KNOWN_COLNAME,
    IS_ADDED_TO_ANKI_COLNAME,
    IS_SUPSENDED_FOR_SOURCE_COLNAME,
    COUNT_COLNAME,
    TO_BE_STUDIED_FROM_DATE_COLNAME,
)
from booktocards.scheduler import Scheduler, KanjiNotKnownError


# =========
# Functions
# =========
def make_ag(df: pd.DataFrame) -> AgGridReturn:
    """Make an ag grid from a DataFrame"""
    grid_option_builder = GridOptionsBuilder.from_dataframe(df)
    grid_option_builder.configure_selection(
        selection_mode="multiple", use_checkbox=True, 
    )
    grid_options = grid_option_builder.build()
    ag_obj = AgGrid(
        df,
        enable_enterprise_modules=False,
        gridOptions=grid_options,
    )
    return ag_obj


def extract_item_and_source_from_ag(
    ag_grid_output: AgGridReturn,
    item_colname: Literal[TOKEN_COLNAME, KANJI_COLNAME],
) -> list[(Union[Token, Kanji], SourceName)]:
    """Extract (item value, source name) info from selected table rows"""
    item_source_couples = []
    for select_row in ag_grid_output.selected_rows:
        couple = [
            select_row[item_colname],
            select_row[SOURCE_NAME_COLNAME],
        ]
        item_source_couples.append(couple)
    return item_source_couples


def get_voc_df_w_date_until(max_date: date, session_state):
    token_df: pd.DataFrame = session_state["kb"][TOKEN_TABLE_NAME]
    out_df = token_df[
        (~token_df[TO_BE_STUDIED_FROM_DATE_COLNAME].isnull())
        & (~token_df[IS_ADDED_TO_ANKI_COLNAME])
    ]
    out_df = out_df[out_df[TO_BE_STUDIED_FROM_DATE_COLNAME] <= max_date]
    return out_df


# =========
# Constants
# =========
# Parameters for card creation
MAX_SOURCE_EX = 3
MAX_TATOEBA_EX = 3
# Keys of the "secrets.yaml" file
SECRETS_DEEPL_KEY_KEY = "deepl_api_key"
# Card attribute names
KANJI_CARD_KANJI_ATTR_NAME = "lemma"
KANJI_CARD_SOURCE_ATTR_NAME = "source_name_str"

# ==================
# Init session state
# ==================
if "kb" not in st.session_state:
    st.session_state["kb"] = KnowledgeBase()
if "sanseido_manipulator" not in st.session_state:
    st.session_state["sanseido_manipulator"] = ManipulateSanseido()
if "tatoeba_db" not in st.session_state:
    st.session_state["tatoeba_db"] = ManipulateTatoeba()
if "deepl_translator" not in st.session_state:
    st.session_state["deepl_translator"] = deepl.Translator(
        io.get_secrets()[SECRETS_DEEPL_KEY_KEY]
    )
if "study_options_are_set" not in st.session_state:
    st.session_state["study_options_are_set"] = False
for df_name in [
    "to_add_tok_df",  # "to_mark_as_known_tok_df", "to_suspend_tok_df",
    "to_add_kanji_df",  # "to_mark_as_known_kanji_df", "to_suspend_kanji_df",
]:
    if df_name not in st.session_state:
        st.session_state[df_name] = pd.DataFrame()
# if "cards_w_date" not in st.session_state:
#    # Pull reserve of tokens to be studied
#    kb: KnowledgeBase = st.session_state["kb"]
#    print(kb[TOKEN_TABLE_NAME])
#    toks: pd.DataFrame = kb[TOKEN_TABLE_NAME][
#        (~ kb[TOKEN_TABLE_NAME][TO_BE_STUDIED_FROM_DATE_COLNAME].isnull()) &
#        (~ kb[TOKEN_TABLE_NAME][IS_ADDED_TO_ANKI_COLNAME])
#    ]
#    tok_cards = [
#        make_voc_cards_from_query(token=tok, source_name=source,
#            translate_source_ex=False, session_state=st.session_state)
#        for tok, source in toks[[TOKEN_COLNAME, SOURCE_NAME_COLNAME]].values
#    ]
#    st.session_state["cards_w_date"] = tok_cards


# ================
# Shared variables
# ================
kb: KnowledgeBase = st.session_state["kb"]
document_names = kb[TOKEN_TABLE_NAME][SOURCE_NAME_COLNAME].unique()
token_df = kb[TOKEN_TABLE_NAME]
kanji_df = kb[KANJI_TABLE_NAME]
seq_df = kb[SEQ_TABLE_NAME]


# ================
# Manage documents
# ================
st.header("Managing documents")
# Descriptive stats
st.subheader("Descriptive stats per document")
min_count = int(
    st.slider(
        label="Count below which a token is not accounted for",
        min_value=1,
        max_value=10,
        value=1,
        step=1,
    )
)
doc_name = st.selectbox(
    label="Document name", options=document_names, key="doc_for_analysis"
)
n_tokens_in_source = token_df.loc[
    (token_df[SOURCE_NAME_COLNAME] == doc_name)
    & (token_df[COUNT_COLNAME] >= min_count),
    COUNT_COLNAME,
].sum()
n_unique_tokens_in_source = token_df[
    (token_df[SOURCE_NAME_COLNAME] == doc_name)
    & (token_df[COUNT_COLNAME] >= min_count)
].shape[0]
n_unique_tokens_in_source_unknown = token_df[
    (token_df[SOURCE_NAME_COLNAME] == doc_name)
    & (token_df[COUNT_COLNAME] >= min_count)
    & (~token_df[IS_KNOWN_COLNAME])
].shape[0]
n_unique_kanjis_in_source = kanji_df[
    kanji_df[SOURCE_NAME_COLNAME] == doc_name
].shape[0]
n_unique_kanjis_in_source_unknown = kanji_df[
    (kanji_df[SOURCE_NAME_COLNAME] == doc_name) & (~kanji_df[IS_KNOWN_COLNAME])
].shape[0]
st.markdown(
    f"###### Descriptive statistics for {doc_name} (latin tokens excluded)\n"
    f"* Number of tokens (appearing {min_count}+ times): {n_tokens_in_source}.\n"
    f"* Number of unique tokens (idem): {n_unique_tokens_in_source} (unknown: {n_unique_tokens_in_source_unknown}).\n"
    f"* Number of unique kanji: {n_unique_kanjis_in_source} (unknown: {n_unique_kanjis_in_source_unknown}).\n"
)
# Add document
st.subheader("Add a document")
doc_name = st.text_input(label="Document name (short)")
if doc_name in [None, ""]:
    st.warning("Enter a document name before uploading")
elif doc_name in document_names:
    st.warning(f"{doc_name} already exists in the database.")
else:
    uploaded_file = st.file_uploader(
        label="Choose a file", key="uploaded_file"
    )
    if uploaded_file is not None:
        stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
        uploaded_text = stringio.read()
        kb.add_doc(
            doc=uploaded_text, doc_name=doc_name, drop_ascii_alphanum_toks=True
        )
        kb.save_kb(make_backup=True)
        st.info("Document added. Reload page.")
# Remove doc
st.subheader("Remove a document")
doc_to_remove = st.selectbox(
    label="Document name", options=document_names, key="doc_to_remove"
)
if st.button("Remove document"):
    kb.remove_doc(doc_name=doc_to_remove)
    st.info(f"Removed {doc_to_remove}. Reload page.")


# ============
# Define study
# ============
st.header("Study settings")
n_days_study = int(
    st.slider(
        label="How many days of study?",
        min_value=1,
        max_value=30,
        value=7,
        step=1,
        key="n_days_study",
    )
)
n_cards_days = int(
    st.slider(
        label="How many new cards a day?",
        min_value=1,
        max_value=30,
        value=6,
        step=1,
        key="n_cards_days",
    )
)
min_count = int(
    st.slider(
        label="What is the lowest count for words to consider?",
        min_value=1,
        max_value=30,
        value=4,
        step=1,
        key="min_count",
    )
)
min_days_btwn_kanji_and_voc = int(
    st.slider(
        label="What is the smallest interval between added a kanji and a related token?",
        min_value=1,
        max_value=30,
        value=22,
        step=1,
        key="min_days_btwn_kanji_and_voc",
    )
)

if st.button("Use these settings for study"):
    st.session_state["scheduler"] = Scheduler(
        kb=kb,
        n_days_study=st.session_state["n_days_study"],
        n_cards_days=st.session_state["n_cards_days"],
        min_days_btwn_kanji_and_voc=st.session_state["min_days_btwn_kanji_and_voc"],
    )


# ================
# Schedule studies
# ================
st.header("Add study material")
# Get scheduler (init if needed)
if "scheduler" not in st.session_state:
    st.session_state["scheduler"] = Scheduler(
        kb=kb,
        n_days_study=st.session_state["n_days_study"],
        n_cards_days=st.session_state["n_cards_days"],
        min_days_btwn_kanji_and_voc=st.session_state["min_days_btwn_kanji_and_voc"],
    )
scheduler: Scheduler = st.session_state["scheduler"]
# Chose doc name
doc_name = st.selectbox(
    label="Document name", options=document_names, key="doc_for_scheduling"
)

# Display studiable items
if len(scheduler.vocab_w_uncertain_status_df) == 0:
    # Allow to mark as known or suspended
    st.subheader("Manage vocabulary")
    if st.button("Mark vocab as known", key="button_voc_known"):
        for token, source_name in st.session_state["selected_tok_src_cples"] :
            kb.set_item_to_known(
                item_value=token,
                item_colname=TOKEN_COLNAME,
                table_name=TOKEN_TABLE_NAME
            )
    if st.button("Mark vocab as suspended", key="button_voc_suspended"):
        for token, source_name in st.session_state["selected_tok_src_cples"] :
            kb.set_item_to_suspended_for_source(
                item_value=token,
                source_name=source_name,
                item_colname=TOKEN_COLNAME,
                table_name=TOKEN_TABLE_NAME
            )
    if st.button("Add to study list", key="button_voc_for_study"):
        for token, source_name in st.session_state["selected_tok_src_cples"] :
            scheduler.add_vocab_of_interest(
                token=token,
                source_name=source_name
            )
    # Show studiable items
    studiable_tokens_df = kb.get_items(
        table_name=TOKEN_TABLE_NAME,
        only_not_added=True,
        only_not_known=True,
        only_not_suspended=True,
        source_name=doc_name,
        )[:20]
    studiable_tokens_df = studiable_tokens_df.sort_values(by=[COUNT_COLNAME],
            ascending=False)
    studiable_tokens_ag = make_ag(df=studiable_tokens_df)
    st.session_state["selected_tok_src_cples"] = extract_item_and_source_from_ag(
        ag_grid_output=studiable_tokens_ag, item_colname=TOKEN_COLNAME,
    )
# If must check kanjis are not known, prompt the user to confirm
else:
    st.subheader("Manage kanjis for added vocabulary")
    # Get kanjis whose status must be confirmed
    if st.button("Mark kanji as known", key="button_kanji_known"):
        for kanji, source_name in st.session_state["selected_kanji_src_cples"]:
            scheduler.add_kanji_for_next_round(
                kanji=kanji,
                source_name=source_name
            )
    if st.button("Add to study list", key="button_kanji_for_study"):
        for kanji, source_name in st.session_state["selected_kanji_src_cples"]:
            scheduler.add_kanji_for_next_round(
                kanji=kanji,
                source_name=source_name
            )
    kanjis_sources_to_check_df = scheduler.get_kanjis_sources_from_token_df(
        token_df=scheduler.vocab_w_uncertain_status_df,
        only_not_added=True,
        only_not_known=True,
        only_not_suspended=True,
    )
    kanjis_sources_to_check_ag = make_ag(df=kanjis_sources_to_check_df)
    st.session_state["selected_kanji_src_cples"] = extract_item_and_source_from_ag(
        ag_grid_output=kanjis_sources_to_check_ag,
        item_colname=KANJI_COLNAME,
    )
    # When all kanjis have been dealt with, try to add to next round, else to
    # rounds after
    if len(kanjis_sources_to_check_df) == 0:
        for token, source_name in scheduler.vocab_w_uncertain_status_df[[TOKEN_COLNAME,
                SOURCE_NAME_COLNAME]].values:
            try:
                # TODO : corriger, puisqu'en l'état ça sera tjrs ajouté (car
                # kanji marqué comme known plutôt que comme added)
                scheduler.add_vocab_for_next_round(token=token,
                        source_name=source_name)
            except KanjiNotKnownError:
                scheduler.add_vocab_for_rounds_after_next(token=token,
                        source_name=source_name)


st.write("Vocab for next round")
st.dataframe(scheduler.vocab_for_next_round_df)
st.write("Kanji for next round")
st.dataframe(scheduler.kanji_for_next_round_df)
st.write("Vocab for rounds after next")
st.dataframe(scheduler.vocab_for_rounds_after_next_df)
st.write("Vocab with uncertain status")
st.dataframe(scheduler.vocab_w_uncertain_status_df)

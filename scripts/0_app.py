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


# =========
# Functions
# =========
def make_ag(df: pd.DataFrame) -> AgGridReturn:
    """Make an ag grid from a DataFrame"""
    grid_option_builder = GridOptionsBuilder.from_dataframe(df)
    grid_option_builder.configure_selection(
        selection_mode="multiple", use_checkbox=True
    )
    grid_options = grid_option_builder.build()
    ag_obj = AgGrid(
        df,
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
# Study parameters
TIME_BTWN_KANJ_AND_TOK = 22  # after adding kanji, time before adding voc
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
st.header("Tokens for study/known")
# Study settings
st.subheader("Study settings")
n_days = int(
    st.slider(
        label="How many days of study?",
        min_value=1,
        max_value=30,
        value=7,
        step=1,
    )
)
n_lessons_day = int(
    st.slider(
        label="How many new cards a day?",
        min_value=1,
        max_value=30,
        value=6,
        step=1,
    )
)
min_count = int(
    st.slider(
        label="What is the lowest count for words to consider?",
        min_value=1,
        max_value=30,
        value=4,
        step=1,
    )
)
min_interval_kanji_voc = int(
    st.slider(
        label="What is the smallest interval between added a kanji and a related token?",
        min_value=1,
        max_value=30,
        value=22,
        step=1,
    )
)


# ================
# Infer study span
# ================
# Nmber of cards to study
num_cards_to_study = n_days * n_lessons_day
# Until when to study?
today = date.today()
last_day_study = today + timedelta(days=n_days)
# If add a knaji, how to long to wait before adding token?
earliest_study_postponed_voc = today + timedelta(days=TIME_BTWN_KANJ_AND_TOK)


# ===================================
# Mandatory cards from previous study
# ===================================
st.subheader("Mandatory cards from previous study")
# Get waiting in queue
vocs_to_add_before_date_df = get_voc_df_w_date_until(
    max_date=last_day_study,
    session_state=st.session_state,
)
vocs_to_add_before_date_df = vocs_to_add_before_date_df.iloc[
    : min(vocs_to_add_before_date_df.shape[0], num_cards_to_study), :
]
# Extract cards
mandatory_cards_to_add_df = pd.DataFrame(
    make_voc_cards_from_df(
        token_df=vocs_to_add_before_date_df,
        translate_source_ex=False,
        session_state=st.session_state,
    )
)
st.write("Cards that will be added from previous selection:")
st.write(mandatory_cards_to_add_df)


# =============
# Show selected
# =============
# kb[TOKEN_TABLE_NAME].loc[10, TO_BE_STUDIED_FROM_DATE_COLNAME] = last_day_study
# kb._save_kb()
# Get tokens that have already been added
st.subheader("Items that will be added")
st.write("Tokens")
st.dataframe(
    pd.concat(
        [
            mandatory_cards_to_add_df,
            st.session_state["to_add_tok_df"],
        ]
    )
)
st.write("Kanjis")
st.dataframe(st.session_state["to_add_kanji_df"])


# =====================
# Check kanji are known
# =====================
st.subheader("Confirm kanji knowledge")
if len(st.session_state["to_add_tok_df"]) != 0:
    kanjis_cards_from_tokens = make_kanji_cards_from_df(
        df=get_kanjis_sources_from_token_df(
            token_df=st.session_state["to_add_tok_df"],
            session_state=st.session_state,
            kanji_colname=KANJI_CARD_KANJI_ATTR_NAME,
            source_name_colname=KANJI_CARD_SOURCE_ATTR_NAME,
        ),
        session_state=st.session_state,
    )
    ag_obj = make_ag(df=pd.DataFrame(kanjis_cards_from_tokens))
    selected_kanji_source_cples = extract_item_and_source_from_ag(
        ag_grid_output=ag_obj, item_colname=KANJI_COLNAME
    )
    st.write(f"Selected kanjis: {selected_kanji_source_cples}")
else:
    st.info("No token in the adding queue for now.")
# TODO: dans kb, mettre un accès filtré aux datasets selon is in anki etc., de
# telle sorte que je peux réutiliser ça partout.

# TODO: mettre en place le mécanisme d'ajout et de sélection
# TODO: bloquer si l'utilisateur n'a pas ajouté/marqué comme connus/suspendus
# tous les kanjis


# =============
# Select tokens
# =============
# Ask whether kanji inside are known or not. If not, add to list of kanji to be
# studied, and make token to be studied in the future
# TODO
# Calculate the number of tokens we still need to pull, and ask user to pull
# that
# TODO
# Select source
# TODO
# Pre-make cards (no deepl translation)
# TODO
# Show cards
# TODO
# When seletcted, user can either mark as known, or add to previous df.
# If already enough tokens, raise error
# TODO
# Should only be cards that have no due date + other conditions
df = token_df[: (n_days * n_lessons_day)]
ag_obj = make_ag(df=df)
selected_tok_source_cples = extract_item_and_source_from_ag(
    ag_grid_output=ag_obj,
    item_colname=TOKEN_COLNAME,
)
st.write(f"Selected token: {selected_tok_source_cples}")
# Write the files somewhere, and *then* mark items as known.
# Download should be made from where the files have been written.
# TODO
# Make a save of kb somewhere (backup)
# TODO


# ===============
# Mark as unknown
# ===============
# TODO: find something supposedly known and make it unknown

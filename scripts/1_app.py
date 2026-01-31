import os
from datetime import date, timedelta
from io import StringIO

import deepl
import pandas as pd
import streamlit as st

from booktocards import io
from booktocards import kb as bk_kb
from booktocards import scheduler as bk_scheduler
from booktocards.aggrid_utils import extract_item_and_source_from_ag, make_ag
from booktocards.jj_dicts import ManipulateSanseido
from booktocards.kb import ColumnName, KnowledgeBase, TableName
from booktocards.scheduler import Scheduler
from booktocards.tatoeba import ManipulateTatoeba


# =========
# Functions
# =========
def get_voc_df_w_date_until(max_date: date, session_state):
    token_df: pd.DataFrame = session_state["kb"][TableName.TOKENS]
    out_df = token_df[
        (~token_df[ColumnName.TO_BE_STUDIED_FROM].isnull())
        & (~token_df[ColumnName.IS_ADDED_TO_ANKI])
    ]
    out_df = out_df[out_df[ColumnName.TO_BE_STUDIED_FROM] <= max_date]
    return out_df


# =========
# Constants
# =========
# Test mode?
TEST_MODE: bool = False
TEST_KB_DIRNAME = "kb_test"
# Parameters for card creation
MAX_SOURCE_EX = 3
MAX_TATOEBA_EX = 3
# Keys of the "secrets.yaml" file
SECRETS_DEEPL_KEY_KEY = "deepl_api_key"
# Keys of the scheduler.yaml
MIN_COUNT_KEY = "min_count"
SCHEDULER_CONF_FILENAME = "scheduler.yaml"
N_DAYS_STUDY_KEY = "n_days_study"
N_CARDS_DAYS_KEY = "n_cards_days"
MIN_DAYS_BTWN_KANJI_AND_VOC_KEY = "min_days_btwn_kanji_and_voc"
# Card attribute names
KANJI_CARD_KANJI_ATTR_NAME = "lemma"
KANJI_CARD_SOURCE_ATTR_NAME = "source_name_str"
# Replacement for linebreaks in examples
EX_LINEBREAK_REPL = " // "


# ==================
# Init session state
# ==================
# Initialize knowledge base
if "kb" not in st.session_state:
    if TEST_MODE:
        kb_dirpath = os.path.join(
            io.get_data_path(),
            "out",
            TEST_KB_DIRNAME,
        )
        st.session_state["kb"] = KnowledgeBase(kb_dirpath=kb_dirpath)
    else:
        st.session_state["kb"] = KnowledgeBase()
# Retrieve scheduler parameters from conf
for scheduler_param in [
    MIN_COUNT_KEY,
    N_DAYS_STUDY_KEY,
    N_CARDS_DAYS_KEY,
    MIN_DAYS_BTWN_KANJI_AND_VOC_KEY,
]:
    if scheduler_param not in st.session_state:
        conf = io.get_conf(SCHEDULER_CONF_FILENAME)
        st.session_state[scheduler_param] = conf[scheduler_param]
for df_name in [
    "to_add_tok_df",  # "to_mark_as_known_tok_df", "to_suspend_tok_df",
    "to_add_kanji_df",  # "to_mark_as_known_kanji_df", "to_suspend_kanji_df",
]:
    if df_name not in st.session_state:
        st.session_state[df_name] = pd.DataFrame()


# ================
# Shared variables
# ================
kb: KnowledgeBase = st.session_state["kb"]
all_document_names = kb.list_doc_names(include_hidden_in_add_full_doc_app=True)
non_hidden_document_names = kb.list_doc_names(include_hidden_in_add_full_doc_app=False)
token_df = kb[TableName.TOKENS]
kanji_df = kb[TableName.KANJIS]
seq_df = kb[TableName.SEQS]


# ================
# Manage documents
# ================
st.header("Managing documents")
# Add document
st.subheader("Add a document")
doc_name = st.text_input(label="Document name (short)")
sep_tok: str | None = st.text_input(label="Special sentence separator?")
if sep_tok == "":
    sep_tok = None
if doc_name in [None, ""]:
    st.warning("Enter a document name before uploading")
elif doc_name in all_document_names:
    st.warning(f"{doc_name} already exists in the database.")
else:
    uploaded_file = st.file_uploader(label="Choose a file", key="uploaded_file")
    if uploaded_file is not None:
        stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
        uploaded_text = stringio.read()
        kb.create_source_entry(source_name=doc_name, hide_in_add_full_doc_app=False)
        try:
            kb.add_doc_from_full_text(
                doc=uploaded_text,
                doc_name=doc_name,
                drop_ascii_alphanum_toks=True,
                sep_tok=sep_tok,
            )
            kb.save_kb(make_backup=True)
            st.info("Document added. Reload page.")
        except Exception as e:
            # Rollback
            kb.remove_doc(doc_name=doc_name)
            raise e
# Remove doc
st.subheader("Remove a document")
doc_to_remove = st.selectbox(
    label="Document name", options=non_hidden_document_names, key="doc_to_remove"
)
if st.button("Remove document"):
    kb.remove_doc(doc_name=doc_to_remove)
    kb.save_kb(make_backup=True)
    st.info(f"Removed {doc_to_remove}. Reload page.")


# ==============================
# Manage token to add on the fly
# ==============================
# TODO: get a special list of sources here
st.header("Manual Token Addition")
token = st.text_input(label="Token to add")
sequence = st.text_input(label="Example sentence (optional)")
source_name = st.selectbox(
    label="Source name",
    options=all_document_names,
    accept_new_options=True,
)
if st.button("Add token to studiable vocabulary"):
    if token.strip() == "":
        st.warning("Token cannot be empty.")
    elif source_name.strip() == "":
        st.warning("Source name cannot be empty.")
    else:
        try:
            if source_name not in all_document_names:
                kb.create_source_entry(
                    source_name=source_name, hide_in_add_full_doc_app=True
                )
            kb.add_token_with_sequence_to_doc(
                token=token,
                sequence=sequence if sequence.strip() != "" else None,
                doc_name=source_name,
                sep_tok=None,
            )
            kb.save_kb(make_backup=False)
            st.success(f'Token "{token}" added to source "{source_name}", kb saved.')
        except bk_kb.NotInJamdictError:
            st.error(f'The token "{token}" does not exist in Jamdict.')
        except bk_kb.TokenAlreadyExistsForSourceInKbError:
            st.error(
                f'The token "{token}" already exists for source "{source_name}" in the knowledge base.'
            )
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")


# ============
# Define study
# ============
st.header("Study settings")
_ = int(
    st.slider(
        label="What is the lowest count for words to consider?",
        min_value=1,
        max_value=10,
        step=1,
        key=MIN_COUNT_KEY,
    )
)
if TEST_MODE:
    added_days_for_test = int(
        st.slider(
            label="How many days later than today should we pretend to be? (for testing)",
            min_value=0,
            max_value=100,
            value=0,
            step=1,
            key="added_days_for_test",
        )
    )
    today = date.today() + timedelta(added_days_for_test)
    st.write(
        f"Today is {today}. The study will span untile"
        f" {today + timedelta(st.session_state[N_DAYS_STUDY_KEY])}."
    )
else:
    today = date.today()

if st.button("Use these settings for study (unsaved changes will be lost)"):
    st.session_state["scheduler"] = Scheduler(
        kb=kb,
        n_days_study=st.session_state[N_DAYS_STUDY_KEY],
        n_cards_days=st.session_state[N_CARDS_DAYS_KEY],
        min_days_btwn_kanji_and_voc=st.session_state[MIN_DAYS_BTWN_KANJI_AND_VOC_KEY],
        today=today,
    )


# ================
# Schedule studies
# ================
st.header("Add study material")
n_shown_tokens = int(
    st.slider(
        label="Number of tokens to show",
        min_value=1,
        max_value=100,
        value=100,
        step=1,
        key="n_shown_tokens",
    )
)

# Get scheduler (init if needed)
if "scheduler" not in st.session_state:
    st.session_state["scheduler"] = Scheduler(
        kb=kb,
        n_days_study=st.session_state[N_DAYS_STUDY_KEY],
        n_cards_days=st.session_state[N_CARDS_DAYS_KEY],
        min_days_btwn_kanji_and_voc=st.session_state[MIN_DAYS_BTWN_KANJI_AND_VOC_KEY],
    )
scheduler: Scheduler = st.session_state["scheduler"]
# Display number of added items
n_added_items = len(scheduler.vocab_for_next_round_df) + len(
    scheduler.kanji_for_next_round_df
)
st.write(
    f"Added {n_added_items}/{scheduler.n_items_to_add + len(scheduler.due_vocab_df)} items"
)
# Chose doc name
doc_name = st.selectbox(
    label="Document name", options=non_hidden_document_names, key="doc_for_scheduling"
)
sort_by_seq_id = st.checkbox(label="Sort by id of first sequence", value=False)
sort_by_count = st.checkbox(label="Sort by count", value=True)
seq_df = kb[TableName.SEQS]
st.write(seq_df[seq_df["seq_id"] == 418])
# token_df=kb.get_items(
#    table_name=TableName.TOKENS,
#    only_not_added=False,
#    only_not_known=False,
#    only_not_suspended=False,
#    only_no_study_date=False,
#    #item_value="頷く",
#    item_colname=ColumnName.TOKEN,
#    source_name=doc_name,
#    max_study_date=None
# )
# st.write(token_df)
# Display studiable items
if len(scheduler.vocab_w_uncertain_status_df) == 0:
    st.subheader("Manage vocabulary")
    # Show studiable items
    studiable_tokens_df = pd.concat(
        [
            # Priority 1, all sources
            scheduler.get_studiable_voc(
                min_count=0,
                sort_seq_id=sort_by_seq_id,
                sort_count=sort_by_count,
                source_name=None,
                priority=2,
            ),
            # Priority 2, selected doc
            scheduler.get_studiable_voc(
                min_count=st.session_state[MIN_COUNT_KEY],
                sort_seq_id=sort_by_seq_id,
                sort_count=sort_by_count,
                source_name=doc_name,
                priority=1,
            ),
        ]
    )
    if len(studiable_tokens_df) > n_shown_tokens:
        studiable_tokens_df = studiable_tokens_df[:n_shown_tokens]
    studiable_tokens_ag = make_ag(df=studiable_tokens_df)
    st.session_state["selected_tok_src_cples"] = extract_item_and_source_from_ag(
        ag_grid_output=studiable_tokens_ag,
        item_colname=ColumnName.TOKEN,
    )
    # Allow to mark as known or suspended
    if st.button("Mark vocab as known", key="button_voc_known"):
        for token, source_name in st.session_state["selected_tok_src_cples"]:
            scheduler.set_vocab_to_add_to_known(
                token=token,
            )
    if st.button("Mark vocab as suspended for this source", key="button_voc_suspended"):
        for token, source_name in st.session_state["selected_tok_src_cples"]:
            scheduler.set_vocab_to_add_to_suspended(
                token=token,
                source_name=source_name,
            )
    if st.button("Add to study list", key="button_voc_for_study"):
        for token, source_name in st.session_state["selected_tok_src_cples"]:
            scheduler.add_vocab_of_interest(token=token, source_name=source_name)
# If must check kanjis are not known, prompt the user to confirm
else:
    st.subheader("Manage kanjis for added vocabulary")
    # Get kanjis whose status must be confirmed
    kanjis_sources_to_check_df = scheduler.get_kanjis_sources_from_token_df(
        token_df=scheduler.vocab_w_uncertain_status_df,
        only_not_added=True,
        only_not_known=True,
        only_not_suspended=True,
        only_not_sched_to_added=True,
        only_not_sched_to_known=True,
        only_not_sched_to_suspended=True,
    )
    kanjis_sources_to_check_ag = make_ag(df=kanjis_sources_to_check_df)
    st.session_state["selected_kanji_src_cples"] = extract_item_and_source_from_ag(
        ag_grid_output=kanjis_sources_to_check_ag,
        item_colname=ColumnName.KANJI,
    )
    # Set status
    if st.button("Mark kanji as known", key="button_kanji_known"):
        for kanji, source_name in st.session_state["selected_kanji_src_cples"]:
            scheduler.set_kanji_to_add_to_known(
                kanji=kanji,
            )
    if st.button("Add to study list", key="button_kanji_for_study"):
        try:
            for kanji, source_name in st.session_state["selected_kanji_src_cples"]:
                scheduler.add_kanji_for_next_round(kanji=kanji, source_name=source_name)
        except bk_scheduler.EnoughItemsAddedError:
            st.info(
                "Enoug items added aldready. Emptied the list of candidate" " vocab."
            )
            scheduler.empty_vocab_w_uncertain_status_df()

    # When all kanjis have been dealt with, try to add to next round, else to
    # rounds after
    if len(kanjis_sources_to_check_df) == 0:
        for token, source_name in scheduler.vocab_w_uncertain_status_df[
            [ColumnName.TOKEN, ColumnName.SOURCE_NAME]
        ].values:
            try:
                scheduler.add_vocab_for_next_round(token=token, source_name=source_name)
            except bk_scheduler.KanjiNotKnownError:
                scheduler.add_vocab_for_rounds_after_next(
                    token=token, source_name=source_name
                )
            except bk_scheduler.EnoughItemsAddedError:
                st.info(
                    "Enough items added already. Emptied the list of candidate"
                    " vocab."
                )
                scheduler.empty_vocab_w_uncertain_status_df()


# =================
# Finish & download
# =================
st.subheader("End scheduling")
if st.button("Finish scheduling", key="end_scheduling"):
    # Get access to third-part tools
    if "sanseido_manipulator" not in st.session_state:
        st.session_state["sanseido_manipulator"] = ManipulateSanseido()
    if "tatoeba_db" not in st.session_state:
        st.session_state["tatoeba_db"] = ManipulateTatoeba()
    if "deepl_translator" not in st.session_state:
        st.session_state["deepl_translator"] = deepl.Translator(
            io.get_secrets()[SECRETS_DEEPL_KEY_KEY]
        )
    # End scheduling (make cards and shift kb)
    out_filepaths = scheduler.end_scheduling(
        translate_source_ex=True,
        sanseido_manipulator=st.session_state["sanseido_manipulator"],
        tatoeba_db=st.session_state["tatoeba_db"],
        for_anki=True,
        deepl_translator=st.session_state["deepl_translator"],
        ex_linebreak_repl=EX_LINEBREAK_REPL,
    )
    st.session_state["out_filepaths"] = out_filepaths
    # Reload kb
    st.session_state["kb"] = KnowledgeBase()

if "out_filepaths" in st.session_state:
    vocab_filepath = st.session_state["out_filepaths"]["vocab"]
    kanji_filepath = st.session_state["out_filepaths"]["kanji"]
    try:
        voc_df = pd.read_csv(vocab_filepath)
    except pd.errors.EmptyDataError:
        pass
    else:
        st.download_button(
            label="Vocabulary cards",
            data=voc_df.to_csv(index=False),
            file_name="vocab.csv",
        )
    try:
        kanji_df = pd.read_csv(kanji_filepath)
    except pd.errors.EmptyDataError:
        pass
    else:
        st.download_button(
            label="Kanji cards",
            data=kanji_df.to_csv(index=False),
            file_name="kanji.csv",
        )


# ======
# Status
# ======
st.subheader("Items")
st.write("Vocab for next round")
st.dataframe(scheduler.vocab_for_next_round_df)
st.write("Kanji for next round")
st.dataframe(scheduler.kanji_for_next_round_df)
st.write("Vocab for rounds after next")
st.dataframe(scheduler.vocab_for_rounds_after_next_df)
st.write("Vocab with uncertain status")
st.dataframe(scheduler.vocab_w_uncertain_status_df)
st.write("Vocab Added to known")
st.write(scheduler.vocab_set_to_add_to_known)
st.write("Vocab Added to suspended")
st.write(scheduler.vocab_set_to_add_to_suspended)
st.write("Kanji Added to known")
st.write(scheduler.kanji_set_to_add_to_known)
st.write("Kanji Added to suspended")
st.write(scheduler.kanji_set_to_add_to_suspended)

import os

import streamlit as st

from booktocards import io
from booktocards.kb import (
    KnowledgeBase,
    NotInJamdictError,
    TokenAlreadyExistsForSourceInKbError,
)
from booktocards.scheduler import Scheduler

st.title("On-the-Fly addition to Knowledge Base")


# =========
# Constants
# =========
# Test mode?
TEST_MODE: bool = True
TEST_KB_DIRNAME = "kb_test"


# ==================
# Init session state
# ==================
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


# ================
# Shared variables
# ================
kb: KnowledgeBase = st.session_state["kb"]
source_names = kb.list_doc_names(include_hidden_in_add_full_doc_app=True)


# ==============
# Token addition
# ==============
st.header("Manual Token Addition")
token = st.text_input(label="Token to add")
sequence = st.text_input(label="Example sentence (optional)")
source_name = st.selectbox(
    label="Source name",
    options=source_names,
    accept_new_options=True,
)
if st.button("Add token to study list"):
    if token.strip() == "":
        st.warning("Token cannot be empty.")
    elif source_name.strip() == "":
        st.warning("Source name cannot be empty.")
    else:
        try:
            if source_name not in source_names:
                kb.create_source_entry(
                    source_name=source_name, hide_in_add_full_doc_app=True
                )
            kb.add_token_with_sequence_to_doc(
                token=token,
                sequence=sequence if sequence.strip() != "" else None,
                doc_name=source_name,
                sep_tok=None,
            )
            st.success(f'Token "{token}" added to source "{source_name}".')
        except NotInJamdictError:
            st.error(f'The token "{token}" does not exist in Jamdict.')
        except TokenAlreadyExistsForSourceInKbError:
            st.error(
                f'The token "{token}" already exists for source "{source_name}" in the knowledge base.'
            )
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

# TODO: add a "validate" button.
# TODO: ensure the "validate" button returns a warning if the token or source_name is
# empty.
# TODO: if validate:
#   1. Create source if not exists (with hide=True)
#   2. Add token to kb. with `add_token_with_sequence_to_doc`
#     a. Handle NotInJamdictError gracefully.
#     b. Handle TokenAlreadyExistsForSourceInKbError gracefully.
# TODO: if the previous worked:
#   1.  scheduler.add_vocab_of_interest(token=token, source_name=source_name)
#   2.  If NoAddableEntryError is raised, tell the user that the token was already added
#       to the study list in the past. They should consider creating a card manually.


# ======================
# Unknown kanji handling
# ======================
# TODO: same as in 1_app.py
# TODO: share scheduler configs across apps?
#  - Or, share only min_days_btwn_kanji_and_voc, and set n_days_study and n_cards_days
#  a large value? (works if no impact of the study date.)
# Get scheduler (init if needed)
if "scheduler" not in st.session_state:
    st.session_state["scheduler"] = Scheduler(
        kb=kb,
        n_days_study=st.session_state["n_days_study"],
        n_cards_days=st.session_state["n_cards_days"],
        min_days_btwn_kanji_and_voc=st.session_state["min_days_btwn_kanji_and_voc"],
    )
scheduler: Scheduler = st.session_state["scheduler"]


# ========
# Finalize
# ========
# TODO: button to save the changes in kb.
# TODO: tell the user to use the main app to get the cards.

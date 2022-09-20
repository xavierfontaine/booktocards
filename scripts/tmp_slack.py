# TODO: simplify: just extracting the "text" field should be enough. It's gonna
# be a bit ugly, but I don't have to keep msgs with URLS.
import json
import os
from dataclasses import dataclass
from typing import Annotated, Optional

SLACK_FOLDER = os.path.join(
    "/home/xavier/Documents/Git/0_xavier_personal/booktocards/data/in",
    "Zeals Slack export Jan 1 2016 - Feb 17 2022",
)


# ================
# Before this file
# ================
# Choose user ids
# TODO
# Choose channels
# TODO


# ===========
# Annotations
# ===========
SlackEntry = Annotated[
    dict,
    """
    One entry of a channel-day level file. This is a message. It can be a
    normal message, an edit of a previous message, a change in channel topic,
    etc.
    See https://slack.com/help/articles/220556107-How-to-read-Slack-data-exports
    """,
]


# ============
# Data classes
# ============
@dataclass
class MessageInfo:
    user_id: str
    original_msg: str
    cleaned_msg: Optional[str] = None
    channel_name: Optional[str] = None


# =========
# Functions
# =========
def extract_text_info(slack_message: dict) -> MessageInfo:
    """
    Extract plain text with rich content removed (url etc.)
    """
    message = slack_message["text"]
    user_id = slack_message["user"]
    # Simplest case: the message is a simple text without rich content
    if "blocks" not in slack_message:
        cleaned_msg = slack_message["text"]
    # ELse: need to exract user text blocks one by one
    else:
        cleaned_msg = ""
        for block in slack_message["blocks"]:
            assert block["type"] == "rich_text", f"unexpected {block['type']=}"
            for element in block["elements"]:
                if element["type"] == "rich_text_section":
                    for e in element["elements"]:
                        if "text" in e:
                            cleaned_msg += e["text"]
    message_info = MessageInfo(
        user_id=user_id, original_msg=message, cleaned_msg=cleaned_msg
    )
    return message_info


# ==========
# File-level
# ==========
json_filepath = os.path.join(SLACK_FOLDER, "dev_chapter_rd/2022-02-17.json")
with open(json_filepath, "r") as f:
    slack_entries: list[SlackEntry] = json.load(f)

# Keep only messages without subtypes
slack_entries = [
    c
    for c in slack_entries
    if (
        c["type"] == "message"  # keeping only messages
        and "subtype"
        not in c  # exlucing deleted msgs, changed versions of messages, bot msgs, etc., see "message subtypes" in https://slack.com/help/articles/220556107-How-to-read-Slack-data-exports#export-file-contents
    )
]
# Keep message if plain text, else keep only elements of rich_text_section
msg_infos = [extract_text_info(c) for c in slack_entries]


# ===============
# Post-treatments
# ===============
# Remove duplicates
# TODO
# Replace user_id by user_name
# TODO
# ELiminate English
# TODO (https://spacy.io/universe/project/spacy_fastlang)


# ==============
# Post this file
# ==============
# Eliminate English lemmas after the text has been parsed in my other code
# TODO
# Add J-J definition
# TODO (Japanese dicts with NLTK: https://www.nltk.org/book-jp/ch12.html)
# Add kanji extraction
# TODO
# Add system to queue kanjis before the words
# TODO

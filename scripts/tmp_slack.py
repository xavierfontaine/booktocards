import ftlangdetect
import json
import os
from dataclasses import dataclass
from typing import Annotated, Optional
import re
import warnings

from booktocards import io as b2c_io


# =========
# Constants
# =========
# Path
SLACK_LOGS_FOLDERNAME = "zeals_slack_2016-01-01_2022-02-17"
SLACK_USER_JSON_FILENAME = "users.json"
# Slack user json - keys
SLACK_USERJSON_ID_KEY = "id"
SLACK_USERJSON_PROFILE_KEY = "profile"
SLACK_USERJSON_REALNAME_KEY = "real_name"
# Slack user ids to focus on
USER_IDS_SUBSET = ["UA7F69DGS"]


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
SlackUserId = Annotated[str, "Slack user ID"]
SlackRealName = Annotated[str, "Slack user 'real name' field"]


# ============
# Data classes
# ============
@dataclass
class MessageInfo:
    user_id: str
    original_msg: str
    msg_wo_user_ref: Optional[str] = None  # without user names and quoted urls
    cleaned_msg: Optional[str] = None  # my version
    channel_name: Optional[str] = None
    lang: Optional[str] = None
    lang_score: Optional[float] = None


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
        msg_wo_user_ref = slack_message["text"]
    # ELse: need to exract user text blocks one by one
    else:
        msg_wo_user_ref = ""
        for block in slack_message["blocks"]:
            assert block["type"] == "rich_text", f"unexpected {block['type']=}"
            for element in block["elements"]:
                if element["type"] == "rich_text_section":
                    for e in element["elements"]:
                        if "text" in e:
                            msg_wo_user_ref += e["text"]
    message_info = MessageInfo(
        user_id=user_id, original_msg=message, msg_wo_user_ref=msg_wo_user_ref
    )
    return message_info

def clean


# ====
# Load
# ====
# Load slack's user json
user_json_path = os.path.join(
    b2c_io.get_data_sources_path(),
    SLACK_LOGS_FOLDERNAME,
    SLACK_USER_JSON_FILENAME
)
with open(user_json_path, "r") as f:
    slack_users_raw: list[dict] = json.load(
        fp=f,
    )


# =================
# Get user_id: name
# =================
user_id_name_lookup: dict[SlackUserId, SlackRealName] = {
    e[SLACK_USERJSON_ID_KEY]: e[SLACK_USERJSON_PROFILE_KEY][SLACK_USERJSON_REALNAME_KEY]
    for e in slack_users_raw
}


# ==========
# File-level
# ==========
#json_filepath = os.path.join(SLACK_FOLDER, "dev_chapter_rd/2022-02-17.json")
json_filepath = "/home/xavier/Documents/Git/booktocards/data/in/sources/zeals_slack_2016-01-01_2022-02-17/cd_salon_team/2022-02-14.json"
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
# Keep only messages from user
slack_entries = [c for c in slack_entries if c["user"] in USER_IDS_SUBSET]
# Keep message if plain text, else keep only elements of rich_text_section
msg_infos = [extract_text_info(c) for c in slack_entries]
# Add language information
for info in msg_infos:
    # Use msg_wo_user_ref because less noise
    msg = info.msg_wo_user_ref
    # Approx: replace linebreaks, not supported by ftlangdetect
    msg = msg.replace("\n", "")
    # Detect and add info
    detected = ftlangdetect.detect(text=msg, low_memory=False)
    info.lang = detected["lang"]
    info.lang_score = detected["score"]
# Drop messages that are not in jp
msg_infos = [m for m in msg_infos if m.lang == "ja"]
# Get the final version of the message
for info in msg_infos:
    info.cleaned_msg = info.original_msg
    # Remove embedded URL (not all URLs)
    info.cleaned_msg = re.sub(pattern="<http.*>", repl="", string=info.cleaned_msg)
    # Replace user id by user name
    for user_id in USER_IDS_SUBSET:
        found_ids = re.findall(
            pattern="<@[A-Z0-9]*>",
            string=info.cleaned_msg
        )
        found_ids = [i[2: -1] for i in found_ids]
        for found_id in found_ids:
            try:
                info.cleaned_msg = re.sub(
                    pattern=f"<@{found_id}>",
                    repl=user_id_name_lookup[found_id],
                    string=info.cleaned_msg
                )
            # If, for some reason, the correspondance isn't found
            except KeyError:
                warnings.warn(
                    f"user id {found_id} not found in the"
                    " {user_id: name} lookup dict."
                )

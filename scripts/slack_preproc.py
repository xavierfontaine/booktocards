"""
Extract and clean slack messages for users user_ids_subset.

Important notes
-  Messages will be separated using MSG_SEPARATOR. This must be used as an
additionnal sentence separator in 0_app.py
- Consider using subsampling so that the output file doesn't exceed 1mb.
  Otherwise, ingestion in other pipes might take log. For simplicity, sampling
  is done at the file level.
"""
from dataclasses import dataclass
import ftlangdetect
import json
import logging
import os
from pathlib import Path
import random
import re
from typing import Annotated, Optional
from tqdm import tqdm
import warnings

from booktocards import io as b2c_io


# =========
# Constants
# =========
# Path
SLACK_USER_JSON_FILENAME = "users.json"
OUT_FILENAME = "slack_extract.txt"
# Slack user json - keys
SLACK_USERJSON_ID_KEY = "id"
SLACK_USERJSON_PROFILE_KEY = "profile"
SLACK_USERJSON_REALNAME_KEY = "real_name"
# Filtering criteria for messages
MIN_MSG_LENGTH: Optional[int] = 15
# Shuffle/subsample
SEED = 42
SAMPLE_PROP: float = .12
# Message separator when written to file (will be used by the sentencizer to
# separate sentences further.)
MSG_SEPARATOR = "-|-"


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


# ======
# Logger
# ======
logging.basicConfig(
    format="[%(levelname)s] %(asctime)s %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)


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


def parse_slack_entries(
    slack_entries: list[SlackEntry],
    user_ids_subset: Optional[list[SlackUserId]] = None,
)->list[MessageInfo]:
    """Parse Slack entries

    Create MessageInfo objects, by 
    1. excluding deleted messages, changed versions
    of messages, bot messages etc.
    2. Keeping only specific users

    Technically, 1 is done by excluding messages with "message subtypes" (see
    https://slack.com/help/articles/220556107-How-to-read-Slack-data-exports#export-file-contents)


    Args:
        user_ids_subset (list[SlackUserId]): user_ids_subset

    Returns:
        list[MessageInfo]:
    """
    # Keep only messages without subtypes
    slack_entries = [
        c
        for c in slack_entries
        if (
            c["type"] == "message"  # keeping only messages
            and "user" in c.keys()  # from a user
            and "subtype" not in c  # exlucing message with subtypes
        )
    ]
    # Keep only messages from specified users
    if user_ids_subset is not None:
        slack_entries = [c for c in slack_entries if c["user"] in user_ids_subset]
    # Keep message if plain text, else keep only elements of rich_text_section
    msg_infos = [extract_text_info(c) for c in slack_entries]
    return msg_infos


def clean_msg_infos(
    msg_infos: list[MessageInfo],
    user_id_name_lookup: dict[SlackUserId, SlackRealName],
)->list[MessageInfo]:
    """Clean list of MessageInfo

    Cleaner messages are stored in MessageInfo.cleaned_msg. Cleaning steps are:
    - Keep only messages in Japanese
    - Remove URLs
    - Replace user ids by "real" user names

    Args:
        msg_infos (list[MessageInfo]): msg_infos
        user_id_name_lookup (dict[SlackUserId, SlackRealName]): user_id_name_lookup

    Returns:
        list[MessageInfo]:
    """
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
    return msg_infos


# ====
# Load
# ====
# Get path to folder log and user 
conf = b2c_io.get_conf("extractor.yaml")
slack_logs_folderpath = conf["slack_logs_folderpath"]
user_ids_subset = conf["user_ids_subset"]
# Load slack's user json
user_json_path = os.path.join(
    slack_logs_folderpath,
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


# ==================
# Create output file
# ==================
out_filepath = os.path.join(
    b2c_io.get_data_sources_path(),
    OUT_FILENAME,
)
logger.info(f"Prepaer output file: {out_filepath}")
# Write to file
with open(out_filepath, "w") as f:
    pass


# ======================
# Extract slack messages
# ======================
logger.info("Extract and clean slack messages")
# Where are the slack logs?
slack_folderpath = os.path.join(
    slack_logs_folderpath,
)
# Get paths
logger.info("-- Get paths")
paths = list(Path(slack_folderpath).rglob('*.json'))
if SAMPLE_PROP == 1.:
    logger.info("-- Shuffle files")
else:
    logger.info(f"-- Shuffle and keep {SAMPLE_PROP} of the files")
random.seed(SEED)
paths = random.sample(population=paths, k=int(SAMPLE_PROP * len(paths)))
# Going through all logs...
logger.info(f"-- Extraction")
if MIN_MSG_LENGTH is not None:
    logger.info(
        f"-- (only messages with {MIN_MSG_LENGTH} chars or more are kept)"
    )
for path in tqdm(paths):
    #... i.e., all json except for non-log jsons
    if path.name not in ["channels.json", "integration_logs.json",
                         "users.json"]:
        with path.open("r") as f:
            slack_entries: list[SlackEntry] = json.load(f)
        # Parse slack entries, keep only users in user_ids_subset
        msg_infos = parse_slack_entries(
            slack_entries=slack_entries,
            user_ids_subset=user_ids_subset,
        )
        # Keep only msgs in Japanese, remove URLs, replace user ids by names
        msg_infos = clean_msg_infos(msg_infos=msg_infos, user_id_name_lookup=user_id_name_lookup)
        # Filter out msgs that are too short
        if MIN_MSG_LENGTH is not None:
            msg_infos = [info for info in msg_infos if len(info.cleaned_msg) >=
                     MIN_MSG_LENGTH]
        # Write
        if msg_infos != []:
            text = (
                MSG_SEPARATOR.join([info.cleaned_msg for info in msg_infos])
                + MSG_SEPARATOR
            )
            with open(out_filepath, "a") as f:
                f.write(text)

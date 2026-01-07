import logging
import os
import pathlib
from datetime import datetime
from functools import reduce
from typing import Literal

import pandas as pd
import tqdm
from jamdict import Jamdict, jmdict

from booktocards import datacl as jp_dataclasses
from booktocards import io, iterables
from booktocards import jamdict_utils as jp_jamdict
from booktocards import spacy_utils as jp_spacy
from booktocards import sudachi as jp_sudachi
from booktocards.datacl import TokenInfo

# TODO:
# Tokenization: replace spacy with sudachi to use the full dict (otherwise,
# long names are cut into two)

# Tokenization:
# - Split at the sentence level -z (id, sent) dataframe
# - For each sentence, create unique (token, sent_id) couples
# - During the ordered counting process, generate (token, count, [sent_id1,
# sent_id2, ...]) (max 3 examples)

# Vocabulary: for a given list of matches (entries)
# - Filter entries: optional: if True, check whether one has a frequent
# kanji/kana form. If there is, filter out entries that don't.
# - Filter kanji forms: option: for each entry, keep only the frequent kanji
# forms if 1+ is frequent (will become the "kanji_forms" list in the card)
# - Filter kana forms: option: for each entry, keep only the frequent kana
# forms if 1+ is frequent (will become 'reading' on the card)

# Vocabulary: for each entry, in vocab_df, create one card where:
# - Below is all IF entry_id NOT in vocab_df
# - lemma: associated lemma
# - entry ID
# - frequent: say whether it is frequent or not
# - vocabulary is the initial token
# - kanji_forms are all the possible kanji forms
# - reading is the list of readings
# - meaning is generated as is in jmdict.JMDEntry.text
# - example id list

# Kanji: for each kanji
# List meanings
# List readings (kun & on)
# - Get 3 vocabularies that use this kanji. Take as many frequent vocab as
# possible.

# Kanji: in kanji_df
# - kanji is the kanji
# - onyomi is the list of onyomi
# - kunyomi is the list of kunyomi
# - ex vocab is a list of 3 vocabs using the kanji

# Selection: pour vocab_df, kanji_df
# - En skippant ceux qui sont pas assez fréquents
# - montrer à partir du premier qui n'est pas "already seen"
# - Tout montrer et demander si on veut garder
# - Si on a dit oui ou non, mettre "already seens"
# - Garder la réponse dans une colonne spécifique

# Order
# - Have a 5 kanjis 25 vocab spacing (by order of the two tables)
#   (add a min() or max() to make sure we can do it)
# - If a vocab appears before its kanji, then make the kanji appear instead,
# and postpone the vocab by min(30 cards, end paquet - current position)

# Write to csv
# - Only where "add_to_deck" is True

# Clean deck
# If word is frequent, quickly turn the card off
# If the word is not frequent, don't turn it off


# ======
# Config
# ======
# Path to input
INPUT_FILEPATH = "scp_test.txt"
# Path to output
OUTPUT_FOLDER = os.path.join(io.get_data_path(), "out")
# Tokenizer to use
EXCLUDED_POS_SUDACHI = [
    # Based on https://github.com/explosion/spaCy/blob/e6f91b6f276e90c32253d21ada29cc20108d1170/spacy/lang/ja/tag_orth_map.py
    # ADP
    ["助詞", "格助詞"],
    ["助詞", "係助詞"],
    ["助詞", "副助詞"],
    # AUX
    ["形状詞", "助動詞語幹"],
    ["助動詞"],
    ["接尾辞", "形容詞的"],
    ["動詞", "非自立可能"],
    ["名詞", "助動詞語幹"],
    # DET
    ["連体詞"],
    # NUM
    ["名詞", "数詞"],
    # PART
    ["助詞", "終助詞"],
    ["接尾辞", "形状詞的"],
    ["接尾辞", "動詞的"],
    # PUNCT & SYM (except some emoji)
    ["補助記号"],
    ["絵文字・記号等"],
    # SCONJ
    ["助詞", "準体助詞"],
    ["助詞", "接続助詞"],
    # SPACE
    ["空白"],
]
SPLIT_MODE = "C"
# Lowest vocab frequency to be considered
LOWEST_FREQ = 1
# Number of example sentences per vocab
N_EX_SENTS = 4
# Should we reorder vocab cards by counts?
REORDER_VOCAB_COUNT = True
# Should we consider linebreaks as additionnal sentence delimiters?
SPLIT_AT_LINEBREAK = True
# Technical - number of rows per sentencized chunked (None if no chunking)
N_LINES_PER_CHUNK = 700


# ======
# Logger
# ======
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ========
# Document
# ========
# TODO: uncomment
# logger.info("Read document")
# with open(INPUT_FILEPATH, "r") as f:
#    doc = f.read()
doc = """SCP-002-JPは厚さ1メートルのコンクリートで覆われた半径8メートル高さ6メートルのドーム状の収容棺にて密閉され、同じ構造を持った収容室内にて管理されています。コンクリート棺は監視カメラによって24時間体制で監視され、異常があった場合はバイオハザードスーツを着用したDクラス職員によって即座に修復がなされます。スーツは焼却処分を行い、携わった職員は米、小麦などの食事を与えた後24時間の間隔離されます。職員に異常が見られた場合は即座に処理を行ってください。"""


# ==============================
# Make (sentence, lemmas) table
# ==============================
logger.info("Sentencize")
# Get sentences
sents = jp_spacy.sentencize(
    doc=doc,
    n_lines_per_chunk=N_LINES_PER_CHUNK,
    split_at_linebreak=SPLIT_AT_LINEBREAK,
)
# For each sentence, tokenize
tokenizer = jp_sudachi.Tokenizer(split_mode=SPLIT_MODE, dict_name="full")
tokenized_sents = [tokenizer.tokenize(doc=sent) for sent in sents]
_ = [
    tokenizer.filter_on_pos(
        dictform_pos_doc=sent,
        excluded_pos=EXCLUDED_POS_SUDACHI,
    )
    for sent in tokenized_sents
]
# Keep only lemmas
sents_lemmas = [[lemma for lemma, _ in sent] for sent in tokenized_sents]
# Table
sents_df = pd.DataFrame({"sent": sents, "lemmas": sents_lemmas})


# ==============================
# Get unique (lemma, count)
# ==============================
logger.info("Get unique (lemma, count)")
# Get tokens and pos
lemmas = reduce(lambda x, y: x + y, sents_df["lemmas"].to_list())
# Get counts
counts = iterables.ordered_counts(it=lemmas)
# Flatten
lemma_counts = [(lemma, count) for lemma, count in counts.items()]
# print(lemma_counts)


# ================
# Drop rare lemmas
# ================
logger.info("Drop rare lemmas")
lemma_counts = [lc for lc in lemma_counts if lc[1] >= LOWEST_FREQ]
# print(lemma_counts)


# ============
# Add sent ids
# ============
logger.info("Add sentence ids")
# For each, get associated sentence ids
lemma_counts_sentids = []
for lemma, count in lemma_counts:
    sent_ids = [idx for idx in sents_df.index if lemma in sents_df.loc[idx, "lemmas"]]
    sent_ids = sent_ids[: min(len(sent_ids), N_EX_SENTS)]
    lemma_counts_sentids.append((lemma, count, sent_ids))
# print(lemma_counts_sentids)

# Create TokenInfo data objects
token_infos = [
    TokenInfo(lemma=lemma, count=count, sent_ids=sent_ids)
    for lemma, count, sent_ids in lemma_counts_sentids
]
# for info in token_infos:
#    print(info)


# =======================
# Get dictionnary entries
# =======================
logger.info("Get dictionnary entries")
for info in token_infos:
    info.dict_entries = jp_jamdict.get_dict_entries(
        query=info.lemma,
        drop_unfreq_entries=True,
        drop_unfreq_readings=True,
        strict_lookup=True,
    )
# for info in token_infos:
#    print(info)


# ==================
# Parse dict entries
# ==================
logger.info("Parse dictionnary entries")
for info in token_infos:
    info.parsed_dict_entries = [
        jp_jamdict.parse_dict_entry(entry=entry) for entry in info.dict_entries
    ]
    # print(info)


# ============
# Create cards
# ============
logger.info("Create cards")
vocab_cards = list()
# Get sentences as {sent_id: sent_str}
sentences = {idx: sents_df.loc[idx, "sent"] for idx in sents_df.index}
# Create all cards
for token_info in token_infos:
    token_cards = jp_dataclasses.token_info_to_voc_cards(
        token_info=token_info, sentences=sentences
    )
    vocab_cards.extend(token_cards)


# ================
# Reorder by count
# ================
if REORDER_VOCAB_COUNT:
    logger.info("Roerder by vocab count")
    sort_index = iterables.argsort(l=[card.count for card in vocab_cards])
    vocab_cards = [vocab_cards[idx] for idx in sort_index]


# for card in vocab_cards:
#    print("\n")
#    print(card)
#    print(card.examples_str)


# ======
# To csv
# ======
logger.info("Write to csv")
vocab_df = pd.DataFrame(vocab_cards)
# Add a column naming the source
filename_stem = pathlib.Path(INPUT_FILEPATH).stem
source_stamp = filename_stem + "-" + datetime.today().strftime("%Y-%m-%d")
vocab_df["source"] = source_stamp
# Replace all newlines by <br/> for ingestion by Anki
vocab_df = vocab_df.replace({"\n": "<br/>"}, regex=True).replace(
    {"\r": "<br/>"}, regex=True
)
# To csv
out_filename = source_stamp + ".csv"
out_filepath = os.path.join(OUTPUT_FOLDER, out_filename)
vocab_df.to_csv(out_filepath, index=False)
exit()


# ==============
# Work on kanjis
# ==============
res = jam.lookup("食べる", strict_lookup=True)
for c in res.chars:
    print(c)
    print(type(c))

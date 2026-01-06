"""
Handle tatoeba corpus
"""
from dataclasses import dataclass
import logging
import os
import json
import pickle
from tatoebatools import tatoeba, ParallelCorpus
from typing import Optional
import tqdm

from booktocards import io
from booktocards.annotations import Token, SentenceId, Sentence, SubPos
from booktocards.sudachi import Tokenizer


# ======
# Logger
# ======
logger = logging.getLogger(__name__)


# =========
# Constants
# =========
TATOEBA_FOLDER = "tatoeba_tanaka_corpus"
_TANAKA_CORPUS_FILENAME = "tanaka_corpus.json"
_INVERTED_INDEX_FILENAME = "inverted_index.json"


# ====
# Core
# ====
_tanaka_folder_path = os.path.join(
    io.get_data_path(),
    "out",
    TATOEBA_FOLDER,
)


class NoProcessedTatoebaFound(Exception):
    """Raise when file cannot be found"""

    pass


@dataclass
class TanakaEntry:
    idx: SentenceId  # ad-hoc index
    sent_jpn: Sentence
    sent_eng: Sentence
    toks_jpn: Optional[list[Token]] = None


class ManipulateTatoeba:
    """ManipulateTatoeba.

    Attributes
        tanaka_par_corpus (dict[SentenceId, TanakaEntry]): list of sentence id
            and Tanaka entries from tatoeba
        inverted_index (dict[Token, list[SentenceId]]): for each token in
            Tanaka, list the associated entries
    """

    def __init__(self) -> None:
        self.tanaka_par_corpus: dict[SentenceId, TanakaEntry] = {}
        self.inverted_index: dict[Token, list[SentenceId]] = {}
        try:
            self._load()
        except NoProcessedTatoebaFound:
            logger.info(
                "-- No prepared corpus found. Making corpus and index."
            )
            self._make_corpus_and_index()
            self._save()

    def _make_corpus_and_index(
        self,
    ) -> None:
        """Extract and prepare Tanaka corpus + {token: sentence ids} index

        The output is attached to self.tanaka_par_corpus and
        self.inverted_index
        """
        # Get parellel corpus
        par_corpus = ParallelCorpus(
            source_language_code="jpn",
            target_language_code="eng",
            update=False,
        )
        # Get sentence ids  for tanaka corpus
        tanaka_ents_ids = [
            sent.sentence_id
            for sent in tatoeba.tags(language="jpn")
            if sent.tag_name == "Tanaka Corpus"
        ]
        # Get (idx, sent_jp, sent_eng) for all sentences in tanaka corpus (idx is
        # ad hoc)
        logger.info("-- Get all sentences from Tanaka Corpus (~250K it)")
        tanaka_par_corpus: dict[SentenceId, TanakaEntry] = {
            idx: TanakaEntry(
                idx=idx,
                sent_jpn=par_sents[0].text,
                sent_eng=par_sents[1].text,
            )
            for idx, par_sents in tqdm.tqdm(enumerate(par_corpus))
            if par_sents[0].sentence_id in tanaka_ents_ids
        }
        # tokenize japanese version of tanaka
        logger.info("-- Tokenize jp sentences")
        tokenizer = Tokenizer()
        lemma_pos_docs = [
            tokenizer.tokenize(doc=doc.sent_jpn)
            for doc in tqdm.tqdm(tanaka_par_corpus.values())
        ]
        # Flter by POS
        logger.info("-- Filter by POS")
        lemma_pos_docs = [
            tokenizer.filter_on_pos(
                dictform_pos_doc=sent,
            )
            for sent in tqdm.tqdm(lemma_pos_docs)
        ]
        # Keep only lemmas
        lemma_docs = [[lemma for lemma, _ in doc] for doc in lemma_pos_docs]
        del lemma_pos_docs
        # Put into the TanakaEntry objects
        for i, k in enumerate(tanaka_par_corpus.keys()):
            tanaka_par_corpus[k].toks_jpn = lemma_docs[i]
        # Create inverted index
        logger.info("-- Create inverted index")
        inverted_index: dict[Token, list[SentenceId]] = {}
        for _, entry in tqdm.tqdm(tanaka_par_corpus.items()):
            assert entry.toks_jpn is not None
            for tok in entry.toks_jpn:
                if tok in inverted_index:
                    inverted_index[tok].append(entry.idx)
                else:
                    inverted_index[tok] = [entry.idx]
        # Associate to self
        self.tanaka_par_corpus = tanaka_par_corpus
        self.inverted_index = inverted_index

    def _save(self, folder_path: str = _tanaka_folder_path) -> None:
        """Save attributes to json"""
        if len(self.tanaka_par_corpus) == 0 or len(self.inverted_index) == 0:
            raise ValueError(
                "Generate the corpus with `self._make_corpus_and_index` first"
            )
        # Serialize the tanaka corpus
        serialized_takana = {
            idx: (entry.sent_jpn, entry.sent_eng, entry.toks_jpn)
            for idx, entry in self.tanaka_par_corpus.items()
        }
        # Save to json
        for filename, obj in zip(
            [_TANAKA_CORPUS_FILENAME, _INVERTED_INDEX_FILENAME],
            [serialized_takana, self.inverted_index],
        ):
            filepath = os.path.join(folder_path, filename)
            with open(filepath, "w") as f:
                json.dump(
                    obj=obj,
                    fp=f,
                )
            logger.info(f"-- Saved {filename} in {filepath}")

    def _load(self, folder_path: str = _tanaka_folder_path) -> None:
        """Load attributes from json"""
        # Get tanaka and deserialize
        filepath = os.path.join(folder_path, _TANAKA_CORPUS_FILENAME)
        if not os.path.isfile(filepath):
            raise NoProcessedTatoebaFound(f"-- No file at {filepath}")
        with open(filepath, "r") as f:
            serialized_tanaka = json.load(
                fp=f,
            )
        self.tanaka_par_corpus = {
            int(idx): TanakaEntry(
                idx=int(idx),
                sent_jpn=sent_jpn,
                sent_eng=sent_eng,
                toks_jpn=toks_jpn,
            )
            for idx, [
                sent_jpn,
                sent_eng,
                toks_jpn,
            ] in serialized_tanaka.items()
        }
        # Get inverted index
        filepath = os.path.join(folder_path, _INVERTED_INDEX_FILENAME)
        with open(filepath, "r") as f:
            self.inverted_index = json.load(
                fp=f,
            )

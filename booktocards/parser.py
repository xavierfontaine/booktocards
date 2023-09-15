"""
Parsers for file and documents
"""
import pandas as pd
import logging
from functools import reduce
from typing import Optional
import tqdm

from booktocards import sudachi as jp_sudachi
from booktocards import spacy_utils as jp_spacy
from booktocards import iterables
from booktocards.annotations import Token, Count, SentenceId, Sentence


# ======
# Logger
# ======
logger = logging.getLogger(__name__)


# =========
# Constants
# =========
_N_LINES_PER_CHUNK = 500


# ====
# Core
# ====
class ParseDocument:
    """Parse a document

    Upon instanciation, extract all sentences and unique tokens from `doc`

    Arguments
        doc (str)

    Attributes
        tokens (dict[Token, [Count, list[SentenceId]]]): list of unique tokens in
            the doc.
        sentences (dict[SentenceId, [Sentence, list[Token]]]): `sent` and their associated `lemmas`
    """

    def __init__(self, doc: str, sep_tok: Optional[str] = None):
        self.tokens: dict[Token, [Count, list[SentenceId]]] = []
        self.sentences: dict[
            SentenceId, [Sentence, list[Token]]
        ] = pd.DataFrame()
        self._sep_tok = sep_tok
        # Parse the doc and ill the above
        self._extract_tokens(doc=doc)

    def _extract_tokens(
        self, doc: str
    ) -> list[Token, Count, list[SentenceId]]:
        """Fill self.tokens and self.sentences"""
        # Sentencize
        logger.info("-- Sentencize")
        sents = list(
            jp_spacy.sentencize(
                doc=doc,
                n_lines_per_chunk=_N_LINES_PER_CHUNK,
                sep_tok=self._sep_tok,
            )
        )
        # Tokenize
        logger.info("-- Tokenize")
        tokenizer = jp_sudachi.Tokenizer()
        tokenized_sents = [
            tokenizer.tokenize(doc=sent) for sent in tqdm.tqdm(sents)
        ]
        # Filter on pos
        _ = [
            tokenizer.filter_on_pos(
                dictform_pos_doc=sent,
            )
            for sent in tokenized_sents
        ]
        # Keep only lemmas
        sents_lemmas = [
            [lemma for lemma, _ in sent] for sent in tokenized_sents
        ]
        # Temporary store for full sentences and their lemmas
        sents_df = pd.DataFrame({"sent": sents, "lemmas": sents_lemmas})
        # Get unique (lemma, count)
        logger.info("Get unique (lemma, count)")
        lemmas = reduce(lambda x, y: x + y, sents_df["lemmas"].to_list())
        counts = iterables.ordered_counts(it=lemmas)
        lemma_counts = [(lemma, count) for lemma, count in counts.items()]
        # Make sents_dict (for return later)
        sents_dict: dict[SentenceId, [Sentence, list[Token]]] = {
            idx: [sent, toks]
            for idx, [sent, toks] in zip(sents_df.index, sents_df.values)
        }
        # For each, get associated sentence ids
        logger.info("Add sentence ids")
        lemma_counts_sentids: dict[Token, [Count, list[SentenceId]]] = {
            lemma: [count, []] for lemma, count in lemma_counts
        }
        for sent_id, [sent, toks] in tqdm.tqdm(sents_dict.items()):
            _ = [lemma_counts_sentids[tok][1].append(sent_id) for tok in toks]
        for value in lemma_counts_sentids.values():
            value[1] == list(set(value[1]))
        # Attach to self
        self.tokens = lemma_counts_sentids
        self.sentences = sents_dict

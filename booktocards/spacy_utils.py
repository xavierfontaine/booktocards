import itertools as it
import logging
import math
from typing import Iterable, List, Optional, Tuple

import spacy
import tqdm

from booktocards.annotations import DictForm, Pos, Sentence, Token

# ======
# Logger
# ======
logger = logging.getLogger(__name__)


# ====
# Core
# ====
def list_jp_pos_tags():
    """List Spacy's POS tags for Japanese"""
    nlp = spacy.load("ja_core_news_sm")
    raw_labels = nlp.get_pipe("morphologizer").labels
    labels = [label.replace("POS=", "") for label in raw_labels]
    return labels


class Tokenizer:
    def __init__(
        self,
        split_mode: str,
        spacy_model: str = "ja_core_news_sm",
    ):
        # Load spacy model. 2 steps necessary to use split_mode
        nlp = spacy.load(
            name=spacy_model,
        )
        nlp = nlp.from_config(config={"nlp": {"tokenizer": {"split_mode": split_mode}}})
        # Attach as attribute
        self.nlp = nlp

    def tokenize(
        self,
        doc: str,
        excluded_pos: List[Pos] = [],
    ) -> List[Tuple[Token, DictForm, Pos]]:
        # Tokenize
        spacified_doc = self.nlp(doc)
        tok_dictform_pos = [
            (tok.text, tok.lemma_, tok.pos_)
            for tok in spacified_doc
            if tok.pos_ not in excluded_pos
        ]
        return tok_dictform_pos


def sentencize(
    doc: str,
    spacy_model: str = "ja_core_news_sm",
    sep_tok: Optional[str] = None,
    n_lines_per_chunk: Optional[int] = None,
    split_at_linebreak: bool = False,
) -> Iterable[Sentence]:
    """Split text into sentences

    Sentence splitting ins performed through SpaCy. SpaCy tend to preserve
    linebreaks inside sentences. Setting `split_at_linebreak` to True will add
    a second pass of split through linebreaks.

    Long documents may trigger a "Tokenization error". In that case, split the
    text in chunks through `n_lines_per_chunk` until the error disappears.

    Args:
        doc (str): doc
        spacy_model (str): spacy_model
        sep_tok (str): special token used to designated chunk separation.
            If sep_tok is set, `n_lines_per_chunk` won't be used.
        n_lines_per_chunk (Optional[int]): n_lines_per_chunk
        split_at_linebreak (bool): split_at_linebreak

    Returns:
        Iterable[Sentence]
    """
    # Get sentencizer
    nlp = spacy.load(
        name=spacy_model,
        exclude=[
            "tok2vec",
            "morphologizer",
            "parser",
            "attribute_ruler",
            "ner",
        ],
    )
    nlp.add_pipe("sentencizer")
    # Get chunks...
    if n_lines_per_chunk is not None and sep_tok is None:
        # ... based on line breaks
        logger.info(f"Split the document into chunks of {n_lines_per_chunk} lines.")
        doc_chunks = _chunkify_on_linebreaks(
            doc=doc,
            n_lines_per_chunk=n_lines_per_chunk,
        )
    elif sep_tok is not None:
        # ... or on a separation token
        logger.info(f"Split the document into chuncks using the separator {sep_tok}.")
        doc_chunks = doc.split(sep=sep_tok)
    else:
        # ... or don't
        doc_chunks = [doc]
    # Sentencize from spacy nlp object
    spacified_doc_chunks = [
        nlp(chunk) for chunk in tqdm.tqdm(doc_chunks, desc="Sentencize")
    ]
    chunks_sents = [
        str(sent).strip() for chunk in spacified_doc_chunks for sent in chunk.sents
    ]
    sents: Iterable[str] = it.chain(chunks_sents)
    # Further sentencize wrt line breaks
    if split_at_linebreak:
        sents = [line for sent in sents for line in sent.splitlines() if line != ""]
    # Further sentencize wrt sep_tok
    if sep_tok is not None:
        sents = [
            sep_sent
            for sent in sents
            for sep_sent in sent.split(sep_tok)
            if sep_sent != ""
        ]
    return sents


def _chunkify_on_linebreaks(
    doc: str,
    n_lines_per_chunk: int,
) -> List[str]:
    """
    Create text chunks, each chunk containing at most `n_lines_per_chunk` lines
    """
    chunks: List[str] = list()
    # Split in lines wrt to linebreaks
    lines = doc.splitlines(keepends=True)
    n_lines = len(lines)
    n_chunks = math.ceil(n_lines / n_lines_per_chunk)
    # Get chunks
    for chunk_idx in range(0, n_chunks):
        chunk = "".join(
            lines[
                (chunk_idx * n_lines_per_chunk) : min(
                    (chunk_idx + 1) * n_lines_per_chunk, n_lines
                )
            ]
        )
        chunks.append(chunk)
    return chunks

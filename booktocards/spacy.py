import spacy
from typing import List, Tuple

from booktocards.annotations import DictForm, Pos, Token, Sentence


def list_jp_pos_tags():
    """List Spacy's POS tags for Japanese"""
    nlp = spacy.load("ja_core_news_sm")
    raw_labels = nlp.get_pipe("morphologizer").labels
    labels = [l.replace("POS=", "") for l in raw_labels]
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
        nlp = nlp.from_config(
            config={"nlp": {"tokenizer": {"split_mode": split_mode}}}
        )
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

def sentencize(doc: str, spacy_model="ja_core_news_sm") -> List[Sentence]:
    # Get sentencizer
    nlp = spacy.load(
        name=spacy_model,
    )
    nlp.add_pipe("sentencizer")
    # Split
    spacified_doc = nlp(doc)
    sents = [str(sent).strip() for sent in spacified_doc.sents]
    # Further split wrt line breaks
    sents = [l for sent in sents for l in sent.splitlines() if l != ""]
    return sents

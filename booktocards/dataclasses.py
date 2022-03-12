from dataclasses import dataclass
from typing import List, Optional, Dict
from jamdict import jmdict

from booktocards.annotations import Pos, Count, SentenceId, Sentence


@dataclass
class ParsedDictEntry:
    """Parsed jamdict.jmdict.JMDEntry"""

    entry_id: Optional[int] = None
    kana_forms: Optional[List[str]] = None
    is_frequent: Optional[bool] = None
    kanji_forms: Optional[List[str]] = None
    meanings: Optional[List[str]] = None


@dataclass
class TokenInfo:
    lemma: str
    pos: Optional[str] = None
    count: Optional[int] = None
    dict_entries: Optional[List[jmdict.JMDEntry]] = None
    parsed_dict_entries: Optional[List[ParsedDictEntry]] = None
    sent_ids: Optional[List[SentenceId]] = None


@dataclass
class VocabCard:
    entry_id: int
    lemma: str
    count: Optional[int] = None
    kana_forms_str: Optional[str] = None
    kanji_forms_str: Optional[str] = None
    is_frequent: Optional[bool] = None
    meanings_str: Optional[str] = None
    examples_str: Optional[str] = None


def token_info_to_voc_cards(
    token_info: TokenInfo, sentences: Dict[SentenceId, Sentence]
) -> List[VocabCard]:
    """One dict per entry in token_info

    Args:
        token_info (TokenInfo)
        sentences (Dict[SentenceId, Sentence]): all sentences, ids
            corresponding to those used in TokenInfo.sent_ids

    Returns:
        List[VocabCard]
    """
    cards = list()
    # Transform example ids to example list
    token_sents = [sentences[sent_id] for sent_id in token_info.sent_ids]
    # Create each card
    for entry in token_info.parsed_dict_entries:
        card = VocabCard(entry_id=entry.entry_id, lemma=token_info.lemma)
        card.count = token_info.count
        card.kana_forms_str = ", ".join(entry.kana_forms)
        card.kanji_forms_str = ", ".join(entry.kanji_forms)
        card.is_frequent = entry.is_frequent
        card.meanings_str = (
            "# " + "\n# ".join(entry.meanings)
            if len(entry.meanings) > 1
            else entry.meanings[0]
        )
        card.examples_str = (
            "# " + "\n# ".join(token_sents)
            if len(token_sents) > 1
            else token_sents[0]
        )
        cards.append(card)
    return cards

from typing import Optional
import sudachipy as sp
from typing import List, Tuple
from booktocards.annotations import DictForm, Pos, SubPos


# =========
# Constants
# =========
# Default constants
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
DICT_NAME = "full"


# =========
# Functions
# =========
class Tokenizer:
    """Sudachi-based tokenizer

    Args:
        split_mode (str): one of "A", "B", "C"
        dict_name: "small", "core" or "full"
    """

    def __init__(
        self,
        split_mode: str = SPLIT_MODE,
        dict_name: str = DICT_NAME,
    ) -> None:
        self.split_mode = getattr(sp.SplitMode, split_mode)
        self.tokenizer = sp.Dictionary(dict=dict_name).create()

    def tokenize(
        self,
        doc: str,
    ) -> List[Tuple[DictForm, List[SubPos]]]:
        """Return tokenized document as dictionary forms and POS.

        Args:
            doc (str): doc
            split_mode (str): one of "A", "B", "C"
            dict_name: "small", "core" or "full"

        Returns:
            List[Tuple[DictForm, List[SubPos]]]:
        """
        morphemes = self.tokenizer.tokenize(text=doc, mode=self.split_mode)
        dictform_pos_doc = [
            (m.dictionary_form(), m.part_of_speech()) for m in morphemes
        ]
        return dictform_pos_doc

    def filter_on_pos(
        self,
        dictform_pos_doc: List[Tuple[DictForm, List[SubPos]]],
        excluded_pos: list[list[SubPos]] = EXCLUDED_POS_SUDACHI,
    ):
        """Remove in-place entries that have pos matching excluded_pos

        Args:
            excluded_pos: excluded POS. For sudachipy, POS take the form
            ('名詞', '普通名詞', 'サ変可能', '*', '*', '*'). Each element of
            excluded_pos can have arbitrary length. If len(excluded_pos[i]) == 1,
            then all tokens with pos[0] = excluded_pos[i][0] will be excluded.
            If length 2, then exclusion further requires pos[1] =
            excluded_pos[i][1].

        Returns:
            None
        """
        # Exclude based on pos (reverse order, not to burn the bridge we are
        # walking on)
        for exc_pos in excluded_pos:
            for lemma_idx in reversed(range(len(dictform_pos_doc))):
                lemma_pos = dictform_pos_doc[lemma_idx][1]
                # If match :exlcude
                if self._check_match_all_criteria(
                    lemma_pos=lemma_pos, pos_conditions=exc_pos
                ):
                    dictform_pos_doc.pop(lemma_idx)
        return dictform_pos_doc

    @staticmethod
    def _check_match_all_criteria(lemma_pos=list[SubPos], pos_conditions=list[SubPos]):
        """pos_conditions can be shorter than lemma_pos (see docstring for
        `tokenize`)"""
        n_pos_conditions = len(pos_conditions)
        for i in range(n_pos_conditions):
            # If mismatch
            if lemma_pos[i] != pos_conditions[i]:
                return False
            # If match
            else:
                # Was already the last condition: True
                if i == (n_pos_conditions - 1):
                    return True
                # Not yet the last: got to the next
                else:
                    continue

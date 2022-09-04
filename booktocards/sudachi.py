import sudachipy as sp
from typing import List, Tuple
from booktocards.annotations import DictForm, Pos, SubPos


# =========
# Functions
# =========
def tokenize(
    doc: str, split_mode: str, dict_name="full",
) -> List[Tuple[DictForm, List[SubPos]]]:
    """Return tokenized document as dictionary forms and POS.

    Args:
        doc (str): doc
        split_mode (str): one of "A", "B", "C"
        dict_name: "small", "core" or "full"

    Returns:
        List[Tuple[DictForm, List[Pos]]]:
    """
    split_mode = getattr(sp.SplitMode, split_mode)
    tokenizer = sp.Dictionary(dict=dict_name).create()
    morphemes = tokenizer.tokenize(text=doc, mode=split_mode)
    dictform_pos_doc = [
        (m.dictionary_form(), m.part_of_speech()) for m in morphemes
    ]
    return dictform_pos_doc

def filter_on_pos(
    dictform_pos_doc: List[Tuple[DictForm, List[SubPos]]],
    excluded_pos: List[List[SubPos]] = [],
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
            if _check_match_all_criteria(lemma_pos=lemma_pos,
                    pos_conditions=exc_pos):
                dictform_pos_doc.pop(lemma_idx)


def _check_match_all_criteria(lemma_pos = list[SubPos], pos_conditions = list[SubPos]):
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

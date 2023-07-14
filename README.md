# BookToCards
Learning Japanese kanjis and vocabulary from any material.

:warning: work-in-progress :warning:


## Installation
1. At the root of the repo, `poetry install`
2. `python -m spacy download ja_core_news_sm`
3. `pip install fasttext-langdetect==1.0.5`
4. Download the 三省堂スーパー大辞林 and place it into `data/in/dictionnaries`

## Run
```bash
streamlit run scripts/1_app.py
```

## Functionalities
Creates Japanese flashcards from material.

### UI 
* Upload of user material → automatic parsing and storage (lemmas, kanjis, sentences.)
* Ordering of lemma by text frequency or order of appearance.
    * User knowledge is stored → knowned/studied lemmas are not proposed.
* User selection of lemmas to learn.
    * Kanji knowledge of composing lemmas is checked.
    * Unknown kanjis are prioritized, associated lemmas are learnable 3 weeks studying kanji.
* Generation of flashcards from learning list.

### Generated cards
Populated fields
* Writings and readings.
* English and Japanese definitions (jamdict, sanseido.)
* Commonness (count in material, rarity, JLPT level.)
* Examples
    * From material + automatic translation.
    * From the tatoeba project.

### Misc
* Parser for slack logs.

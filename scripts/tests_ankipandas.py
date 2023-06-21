# 4 DB de cartes à ajouter:
# - Anki collection: cartes qui sont new, ne sont pas suspendus, et qui n'ont pas le tag
# "augmentées"
# - cartes extraites : cartes qui n'ont pas déjà été ajoutées, dont le champ
# identifiant n'existe pas dans la collection anki, et qui n'ont pas été
# marquées comme connues
# - cartes en attente : cartes prêtes à ajouter, mais qui ne doivent pas être
# ajoutées avant une certaine data
# - kanjis connus : liste de kanjis déjà étudiés

# Système:
# - On load les 3 collections ci-dessus.
# - Dans un premier onglet, on permet à l'user de marquer les cartes connues
# (les kanjis dans les cartes connues sont automatiquement marqués comme connues)
# - On demande à l'user combien de cartes/jour il fait.
# - On demande à l'user pour combien de jours il veut adder des cartes.
# - S'il y a des cartes en attente qui doivent apparaître dans l'intervale, on
# les ajoute d'office (mises à la fin des nouvelles cartes, histoire de
# simplifier)
# - On demande à l'user d'ajouter des cartes depuis l'Ankicol et depuis les
# cartes extraites (check que le compte est bon)
# - On traite toutes les cartes
#   * Si plusieurs lectures trouvées pour une acrte, on garde la première et on
#   raise un warning
# - On montre les modifs
# - On demande à l'user de valider

import ankipandas as ap

anki_conf = {
    "user": "ユーザー 1",
    "deck_path": "/home/xavier/Git/fukubukuro/data/Anki2/",
    "used_decks": [
        "Archive\x1fWaniKani Ultimate\x1fVocabulary",
        "中級１- kanji\x1f中級１- vocab (kanji)",
        "中級２- vocab",
        "中級２ - kanji",
        "中級２ -  Yomichan (current)",
        "Archive\x1fRetired items",
        "上級１- kanji",
        "中級１ - vocab (hiragana)",
        "Archive\x1fWaniKani Ultimate\x1f-Radicals",
        "中級１- kanji\x1f中級１- radicaux",
        "Archive\x1fWaniKani Ultimate\x1fKanjis",
        "中級１- kanji\x1f中級１- kanji reading",
        "上級１- 文法",
        "Archive\x1fDS",
        "Mons - chap32-50",
        "System",
        "初級２- vocabulaire",
        "Archive\x1fSNG - recall",
        "初級２- grammaire",
        "Archive\x1f初級２- kanji",
        "Archive\x1f中級１ - KANJI 1 - A INTEGRER",
        "中級１- kanji\x1f中級１ - writing",
        "中級１ - grammaire",
        "Archive\x1fLeisure",
        "中級２ - grammaire",
    ],
}


col = ap.Collection(user=anki_conf["user"], path=anki_conf["deck_path"])
notes = col.notes.fields_as_columns()

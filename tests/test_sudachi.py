from booktocards.sudachi import filter_on_pos

def test_exclusion_work():
    dictform_pos_doc = [
        ("lemma1", ("noun", "bla", "bla", "bla")),
        ("lemma2", ("adv", "bla", "bla", "bla")),
        ("lemma3", ("adj", "fem", "bla", "bla")),
        ("lemma4", ("adj", "masc", "bla", "bla")),
        ("lemma5", ("past participle", "fem", "bla", "bla")),
    ]
    excluded_pos = [
        ["noun",],
        ["adj", "fem"],
    ]
    exp_out = [
        ("lemma2", ("adv", "bla", "bla", "bla")),
        ("lemma4", ("adj", "masc", "bla", "bla")),
        ("lemma5", ("past participle", "fem", "bla", "bla")),
    ]
    obs_out = filter_on_pos(dictform_pos_doc=dictform_pos_doc,
            excluded_pos=excluded_pos)
    assert exp_out == obs_out

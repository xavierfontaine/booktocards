import unittest
from jamdict import Jamdict
from booktocards.jamdict_utils import (
    drop_unfrequent_entries,
    drop_unfrequent_readings,
)


class TestHasFrequentReading(unittest.TestCase):
    def test_works_when_has_frequent_reading(self):
        """初 has 3 entries, and only first is frequent"""
        jam = Jamdict()
        entries = jam.lookup("初", strict_lookup=True).entries
        # Make sure the dict is as expected
        assert len(entries) == 3, "Test issue: dict has changed"
        # Test
        exp_out = [entries[0]]
        obs_out = drop_unfrequent_entries(entries=entries)
        assert exp_out == obs_out

    def test_works_when_no_frequent_reading(self):
        jam = Jamdict()
        entries = jam.lookup("自由研究", strict_lookup=True).entries
        exp_out = entries
        obs_out = drop_unfrequent_entries(entries=entries)
        assert exp_out == obs_out


class TestDropUnfrequentReadings(unittest.TestCase):
    def test_work_when_has_frequent_readings(self):
        """Only the first kana reading in 刁Eis frequent"""
        # Arrange
        jam = Jamdict()
        entry = jam.lookup("初", strict_lookup=True).entries[0]
        # Make sure the dict is as expected
        assert len(entry.kana_forms) == 2, "Test issue: dict has changed"
        assert len(entry.kanji_forms) == 1, "Test issue: dict has changed"

        # Act
        out = drop_unfrequent_readings(entry=entry)

        # Assert
        exp_kanji_out = entry.kanji_forms[0]
        exp_kana_out = entry.kana_forms[0]
        assert len(out.kanji_forms) == 1
        assert len(out.kana_forms) == 1
        assert exp_kanji_out.text == out.kanji_forms[0].text
        assert exp_kana_out.text == out.kana_forms[0].text

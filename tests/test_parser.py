import pytest
from booktocards.parser import ParseDocument


class TestParseDocument:
    """Integration tests for ParseDocument class"""

    def test_simple_document_parsing(self):
        """Test parsing a simple Japanese document"""
        doc = "私は学生です。"
        parser = ParseDocument(doc=doc)

        # Verify that parsing occurred
        assert len(parser.tokens) == 2  # "私" and "学生"
        assert len(parser.sentences) == 1

        # Verify content
        assert parser.tokens == {"私": (1, [0]), "学生": (1, [0])}
        assert parser.sentences == {0: ("私は学生です。", ["私", "学生"])}

    def test_multiple_sentences(self):
        """Test parsing multiple sentences"""
        doc = "私は学生です。彼は先生です。"
        parser = ParseDocument(doc=doc)

        # Verify that parsing occurred
        assert len(parser.tokens) == 4  # "私", "学生", "彼", "先生"
        assert len(parser.sentences) == 2

        # Verify content
        assert parser.tokens == {
            "私": (1, [0]),
            "学生": (1, [0]),
            "彼": (1, [1]),
            "先生": (1, [1]),
        }
        assert parser.sentences == {
            0: ("私は学生です。", ["私", "学生"]),
            1: ("彼は先生です。", ["彼", "先生"]),
        }

    def test_token_counting(self):
        """Test that tokens are counted correctly"""
        # Use a token that appears multiple times
        doc = "食べる。食べる。"
        parser = ParseDocument(doc=doc)

        # Verify content
        assert parser.tokens == {
            "食べる": (2, [0, 1]),
        }
        assert parser.sentences == {
            0: ("食べる。", ["食べる"]),
            1: ("食べる。", ["食べる"]),
        }

    def test_sentence_id_deduplication(self):
        """Test that sentence IDs are deduplicated when token appears multiple times in same sentence"""
        # Document where same token appears multiple times in one sentence
        doc = "食べる食べる食べる"
        parser = ParseDocument(doc=doc)

        # Verify content
        assert parser.tokens == {
            "食べる": (3, [0]),
        }
        assert parser.sentences == {
            0: ("食べる食べる食べる", ["食べる", "食べる", "食べる"]),
        }

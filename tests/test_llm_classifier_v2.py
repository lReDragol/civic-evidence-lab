"""Tests for classifier/llm_classifier_v2.py.

Tests prompt building and result validation/normalization without
requiring live LLM inference.
"""
from __future__ import annotations

import unittest
from unittest.mock import patch


class TestLLMClassifierV2(unittest.TestCase):
    def test_build_prompt_contains_valid_tag_lists(self):
        from classifier.llm_classifier_v2 import _build_prompt, VALID_L1, VALID_L2, VALID_L3, MANIPULATION_TECHNIQUES
        prompt = _build_prompt()
        self.assertIn("court", prompt)
        self.assertIn("politics", prompt)
        self.assertIn("high_risk", prompt)
        self.assertIn("whataboutism", prompt)

    def test_validate_and_normalize_accepts_valid_l1(self):
        from classifier.llm_classifier_v2 import _validate_and_normalize
        result = _validate_and_normalize({"l1": "corruption_claim", "l2": "corruption", "l3": ["possible_corruption"]})
        self.assertEqual(result["l1"], "corruption_claim")
        self.assertEqual(result["l2"], "corruption")
        self.assertEqual(result["l3"], ["possible_corruption"])

    def test_validate_and_normalize_rejects_invalid_l1(self):
        from classifier.llm_classifier_v2 import _validate_and_normalize
        result = _validate_and_normalize({"l1": "invalid_tag", "l2": "unknown", "l3": ["unknown"]})
        self.assertIsNone(result["l1"])
        self.assertIsNone(result["l2"])
        self.assertEqual(result["l3"], [])

    def test_validate_and_normalize_clamps_manipulation_risk(self):
        from classifier.llm_classifier_v2 import _validate_and_normalize
        result = _validate_and_normalize({"manipulation_risk": 1.5})
        self.assertEqual(result["manipulation_risk"], 1.0)
        result = _validate_and_normalize({"manipulation_risk": -0.5})
        self.assertEqual(result["manipulation_risk"], 0.0)

    def test_validate_and_normalize_filters_manipulation_techniques(self):
        from classifier.llm_classifier_v2 import _validate_and_normalize, MANIPULATION_TECHNIQUES
        result = _validate_and_normalize({"manipulation_techniques": ["appeal_to_fear", "nonexistent"]})
        self.assertEqual(result["manipulation_techniques"], ["appeal_to_fear"])

    def test_validate_and_normalize_fixes_sentiment(self):
        from classifier.llm_classifier_v2 import _validate_and_normalize
        result = _validate_and_normalize({"sentiment": "happy"})
        self.assertEqual(result["sentiment"], "neutral")

    def test_validate_and_normalize_preserves_negation(self):
        from classifier.llm_classifier_v2 import _validate_and_normalize
        result = _validate_and_normalize({"is_negated": True})
        self.assertTrue(result["is_negated"])

    def test_validate_and_normalizes_l1_case(self):
        from classifier.llm_classifier_v2 import _validate_and_normalize
        result = _validate_and_normalize({"l1": "Corruption Claim"})
        # Should normalize via lower + replace spaces
        self.assertEqual(result["l1"], "corruption_claim")


if __name__ == "__main__":
    unittest.main()

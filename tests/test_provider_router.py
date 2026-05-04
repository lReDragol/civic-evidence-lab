import unittest
from unittest.mock import patch


class ProviderRouterTests(unittest.TestCase):
    def test_stage_prompt_for_cleaner_forbids_external_web_facts(self):
        from llm.provider_router import _stage_prompt

        system_prompt, _user_prompt = _stage_prompt(
            {"stage": "clean_factual_text", "unit": {"unit_key": "content:1"}, "payload": {"title": "x"}}
        )

        self.assertIn("Do NOT use external web search", system_prompt)
        self.assertIn("Do not add new facts", system_prompt)

    def test_stage_prompt_for_relation_reasoning_allows_web_grounded_validation_only(self):
        from llm.provider_router import _stage_prompt

        system_prompt, _user_prompt = _stage_prompt(
            {"stage": "relation_reasoning", "unit": {"unit_key": "content:1"}, "payload": {"title": "x"}}
        )

        self.assertIn("MAY use web-grounded official or documentary context", system_prompt)
        self.assertIn("do NOT introduce unrelated actors", system_prompt)

    def test_run_ai_task_parses_json_envelope_from_chat_response(self):
        from llm.provider_router import run_ai_task

        fake_response = {
            "choices": [
                {
                    "message": {
                        "content": """```json
{
  "output_text": "Чистый factual текст",
  "output_json": {
    "cleaned_text": "Чистый factual текст",
    "removed_noise": ["cta"]
  },
  "confidence": 0.91
}
```"""
                    }
                }
            ]
        }

        with patch("llm.provider_router._post_json", return_value=fake_response):
            result = run_ai_task(
                provider="perplexity",
                model="sonar-reasoning-pro",
                api_key="fake-px-key",
                task={"stage": "clean_factual_text", "unit": {"unit_key": "content:1"}, "payload": {"title": "x"}},
            )

        self.assertEqual(result["output_text"], "Чистый factual текст")
        self.assertEqual(result["output_json"]["cleaned_text"], "Чистый factual текст")
        self.assertAlmostEqual(result["confidence"], 0.91, places=2)

    def test_run_ai_task_falls_back_to_stage_shape_when_model_returns_plain_text(self):
        from llm.provider_router import run_ai_task

        fake_response = {
            "choices": [
                {
                    "message": {
                        "content": "Событие: ограничение доступа. Участники жалуются на блокировку."
                    }
                }
            ]
        }

        with patch("llm.provider_router._post_json", return_value=fake_response):
            result = run_ai_task(
                provider="groq",
                model="groq/compound",
                api_key="fake-gq-key",
                task={"stage": "event_synthesis", "unit": {"unit_key": "event:1"}, "payload": {"title": "x"}},
            )

        self.assertIn("summary_short", result["output_json"])
        self.assertIn("summary_long", result["output_json"])
        self.assertTrue(result["output_text"])
        self.assertGreaterEqual(result["confidence"], 0.0)

    def test_structured_extract_sends_json_schema_to_mistral_and_marks_valid_schema(self):
        from llm.provider_router import run_ai_task

        captured = {}
        fake_response = {
            "choices": [
                {
                    "message": {
                        "content": """{
  "output_text": "РКН выписал требование.",
  "output_json": {
    "actors": [],
    "organizations": ["Роскомнадзор"],
    "dates": ["30.03.2026"],
    "locations": [],
    "actions": ["privacy_enforcement"],
    "legal_basis": ["152-ФЗ"],
    "affected_groups": [],
    "explicit_claims": ["РКН выписал требование."],
    "uncertainty_markers": [],
    "document_anchors": ["Требование"],
    "source_facts": ["РКН выписал требование."],
    "external_context": []
  },
  "confidence": 0.9
}"""
                    }
                }
            ]
        }

        def fake_post(url, *, headers, payload, timeout=120):
            captured["payload"] = payload
            return fake_response

        with patch("llm.provider_router._post_json", side_effect=fake_post):
            result = run_ai_task(
                provider="mistral",
                model="mistral-medium-2505",
                api_key="fake-mistral-key",
                task={"stage": "structured_extract", "unit": {"unit_key": "content:1"}, "payload": {"title": "x"}},
            )

        self.assertEqual(captured["payload"]["response_format"]["type"], "json_schema")
        self.assertEqual(result["schema_name"], "structured_extract_v1")
        self.assertTrue(result["schema_valid"])
        self.assertEqual(result["schema_errors"], [])

    def test_structured_extract_plain_text_is_schema_invalid(self):
        from llm.provider_router import run_ai_task

        fake_response = {"choices": [{"message": {"content": "plain text, not json"}}]}

        with patch("llm.provider_router._post_json", return_value=fake_response):
            result = run_ai_task(
                provider="perplexity",
                model="sonar-reasoning-pro",
                api_key="fake-px-key",
                task={"stage": "structured_extract", "unit": {"unit_key": "content:1"}, "payload": {"title": "x"}},
            )

        self.assertFalse(result["schema_valid"])
        self.assertIn("missing_json_envelope", result["schema_errors"])


if __name__ == "__main__":
    unittest.main()

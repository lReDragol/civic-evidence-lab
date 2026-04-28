import unittest

from runtime.registry import PIPELINE_JOB_IDS, get_job_spec


class RegistryPipelineTests(unittest.TestCase):
    def test_ai_full_sweep_job_is_registered_as_manual_orchestration_job(self):
        spec = get_job_spec("ai_full_sweep")
        self.assertIsNotNone(spec)
        self.assertFalse(spec.scheduled)
        self.assertEqual(spec.stage, "ai")

    def test_nightly_pipeline_runs_quality_stages_before_snapshot_export(self):
        nightly = PIPELINE_JOB_IDS["nightly"]

        self.assertLess(nightly.index("content_dedupe"), nightly.index("tagger"))
        self.assertLess(nightly.index("tagger"), nightly.index("claim_cluster"))
        self.assertLess(nightly.index("claim_cluster"), nightly.index("semantic_index"))
        self.assertLess(nightly.index("relations"), nightly.index("classifier_audit"))
        self.assertLess(nightly.index("classifier_audit"), nightly.index("quality_gate"))
        self.assertLess(nightly.index("quality_gate"), nightly.index("analysis_snapshot"))
        self.assertLess(nightly.index("analysis_snapshot"), nightly.index("obsidian_export"))


if __name__ == "__main__":
    unittest.main()

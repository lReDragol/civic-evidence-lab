import unittest
from runtime.civic_quality import baseline_report


class CivicQualityTests(unittest.TestCase):
    def test_missing_baseline_and_duplicate_reviews_fail(self):
        self.assertFalse(baseline_report([])["ok"])
        row={"kind":"claim_incident","id":"one","reviewed_by":"human","reviewed_at":"2026-09-20","accepted":True,"correct":True}
        result=baseline_report([row,row])
        self.assertEqual(result["reviewed_counts"]["claim_incident"],1)
        self.assertTrue(result["errors"])

    def test_precision_reports_sample_uncertainty(self):
        rows=[{"kind":kind,"id":str(i),"reviewed_by":"synthetic-test","reviewed_at":"2026-09-20",
               "accepted":True,"correct":True} for kind,n in (("claim_incident",300),("protocol",100),("link_precinct_match",200)) for i in range(n)]
        result=baseline_report(rows)
        self.assertTrue(result["ok"])
        self.assertEqual(result["accepted_sample"],200)
        self.assertLess(result["wilson_95_interval"][0],1)

import sys
import io
from config.db_utils import get_db, load_settings

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
conn = get_db(load_settings())

tables = [
    'content_items', 'claims', 'entities', 'entity_aliases',
    'deputy_profiles', 'bills', 'bill_sponsors', 'bill_vote_sessions',
    'bill_votes', 'official_positions', 'party_memberships',
    'investigative_materials', 'verifications', 'cases',
    'entity_relations', 'law_references', 'tag_explanations',
]

for t in tables:
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {count}")
    except Exception as e:
        print(f"  {t}: ERROR {e}")

conn.close()

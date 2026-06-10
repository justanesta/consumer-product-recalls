from __future__ import annotations

import sqlalchemy as sa

from src.config.db import make_engine
from tests.e2e._dbt_runner import run_dbt


def test_orphan_invariant_fires_on_planted_violation(test_db_url: str) -> None:
    """C30 fixture-violation harness: prove the zero-baseline structural invariant
    ``assert_no_orphan_products`` actually CATCHES a violation, not just that it is green today.

    On the clean prod-clone branch the invariant passes; planting an orphan ``recall_product``
    (a recall_event_id with no matching recall_event) makes it fail. The branch is thrown away on
    teardown, so the planted row never touches prod. Skips without NEON_API_KEY.
    """
    clean = run_dbt(test_db_url, "test", "--select", "assert_no_orphan_products")
    assert clean.returncode == 0, f"invariant should pass on the clean branch:\n{clean.stdout}"

    engine = make_engine(test_db_url)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "insert into recall_product (recall_product_id, recall_event_id, source) "
                "values ('e2e-planted-orphan', 'e2e-nonexistent-event', 'FDA')"
            )
        )

    planted = run_dbt(test_db_url, "test", "--select", "assert_no_orphan_products")
    assert planted.returncode != 0, (
        f"invariant should FAIL after planting an orphan recall_product:\n{planted.stdout}"
    )

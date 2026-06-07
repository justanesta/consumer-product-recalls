"""Tests for the per-source dedup-contract registry and BronzeLoader.from_contract.

The headline test is ``test_nhtsa_record_id_churn_does_not_change_dedup`` — the regression
guard for the NHTSA deep-rescan latent bug. The bug was that the deep-rescan loader keyed
on the regen-unstable ``source_recall_id`` (RECORD_ID) and hashed it, while the incremental
path used the 11-tuple and excluded RECORD_ID from the hash; the two paths disagreed and
re-inserted the same logical row on every file regen. Now both paths resolve ONE contract,
so the disagreement is structurally impossible.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME, DedupContract
from src.bronze.hashing import content_hash
from src.bronze.loader import BronzeLoader
from src.config.source_registry import EXTRACTOR_BY_SOURCE_NAME


def _loader(contract: DedupContract, **overrides: bool) -> BronzeLoader:
    """Build a loader from a contract with throwaway table objects (from_contract only
    stores them; it never introspects them at construction)."""
    return BronzeLoader.from_contract(
        contract,
        bronze_table=MagicMock(),
        rejected_table=MagicMock(),
        **overrides,  # type: ignore[arg-type]
    )


class TestRegistryShape:
    def test_keys_match_extractor_registry(self) -> None:
        """A contract for every source, and no orphan contracts — so every extractor's
        DEDUP_CONTRACT_BY_SOURCE_NAME[source_name] lookup resolves."""
        assert set(DEDUP_CONTRACT_BY_SOURCE_NAME) == set(EXTRACTOR_BY_SOURCE_NAME)

    def test_every_contract_has_nonempty_identity(self) -> None:
        for source, contract in DEDUP_CONTRACT_BY_SOURCE_NAME.items():
            assert contract.identity_fields, f"{source} has empty identity_fields"


class TestNhtsaOracle:
    def test_nhtsa_oracle_is_eleven_tuple_with_record_id_excluded(self) -> None:
        """Pins the ADR 0030 oracle — the single source of truth both NHTSA load paths
        consume. source_recall_id (RECORD_ID) is regen-unstable, so it must be OUT of the
        identity tuple and IN the hash-exclusion set."""
        contract = DEDUP_CONTRACT_BY_SOURCE_NAME["nhtsa"]
        assert contract.identity_fields == (
            "campno",
            "maketxt",
            "modeltxt",
            "yeartxt",
            "compname",
            "rcl_cmpt_id",
            "mfr_comp_ptno",
            "mfr_comp_desc",
            "mfr_comp_name",
            "endman",
            "bgman",
        )
        assert "source_recall_id" not in contract.identity_fields
        assert "source_recall_id" in contract.hash_exclude_fields
        # Incremental defaults carried on the contract (ADR 0030).
        assert contract.default_within_batch_dedup is True
        assert contract.default_allow_null_identity is True

    def test_nhtsa_record_id_churn_does_not_change_dedup(self) -> None:
        """THE regression guard. A NHTSA file regen reassigns RECORD_ID but leaves the
        logical row otherwise identical. Under the contract's oracle that row must produce
        (a) the same identity tuple and (b) the same content_hash — so the deep-rescan
        path is a no-op for it, exactly as the incremental path already was. This mirrors
        the loader's own identity/hash construction (BronzeLoader.load)."""
        contract = DEDUP_CONTRACT_BY_SOURCE_NAME["nhtsa"]
        row = {
            "source_recall_id": "1551234",  # RECORD_ID — the unstable counter
            "campno": "23V456000",
            "maketxt": "FORD",
            "modeltxt": "EXPLORER",
            "yeartxt": "2022",
            "compname": "STEERING",
            "rcl_cmpt_id": "789012",
            "mfr_comp_ptno": "ABC-123",
            "mfr_comp_desc": "STEERING GEAR",
            "mfr_comp_name": "ACME STEERING",
            "endman": "2022-06-01T00:00:00Z",
            "bgman": "2021-01-01T00:00:00Z",
            "desc_defect": "unchanged text",
        }
        regen = {**row, "source_recall_id": "1559999"}  # only RECORD_ID changed

        def identity(d: dict[str, str]) -> tuple[str, ...]:
            return tuple(str(d.get(f, "")) for f in contract.identity_fields)

        def hashed(d: dict[str, str]) -> str:
            return content_hash(
                {k: v for k, v in d.items() if k not in contract.hash_exclude_fields}
            )

        assert identity(row) == identity(regen)  # same identity bucket
        assert hashed(row) == hashed(regen)  # same content_hash → recognized, not re-inserted


class TestFdaPressReleaseOracle:
    def test_oracle_is_full_natural_key(self) -> None:
        """Pins the fda_press_releases oracle to the full 4-column natural key.

        The original 2-tuple (source_recall_id, press_release_url) collided on event 76385,
        which returns the same URL twice (same type "FDA", different issue dates), and
        press_release_issued_dt is content — so the 2-tuple raised
        WithinBatchIdentityCollisionError mid-seed. issued_dt and type must be IN the identity
        to keep the two distinct releases apart (probe 2026-06-07)."""
        contract = DEDUP_CONTRACT_BY_SOURCE_NAME["fda_press_releases"]
        assert contract.identity_fields == (
            "source_recall_id",
            "press_release_url",
            "press_release_type",
            "press_release_issued_dt",
        )
        assert contract.hash_exclude_fields == frozenset()
        assert contract.default_within_batch_dedup is True
        assert contract.default_allow_null_identity is True

    def test_event_76385_same_url_different_issued_dt_no_longer_collides(self) -> None:
        """THE regression guard for the seed crash. Event 76385's two FDA rows share
        (source_recall_id, press_release_url, press_release_type) and differ only in
        press_release_issued_dt. Under the full-natural-key oracle they map to DISTINCT
        identity tuples, so both load and _dedup_within_batch never raises. A true
        byte-duplicate (all four equal) still shares identity AND content_hash → collapses.
        Mirrors the loader's identity/hash construction (BronzeLoader.load)."""
        contract = DEDUP_CONTRACT_BY_SOURCE_NAME["fda_press_releases"]

        def identity(d: dict[str, str]) -> tuple[str, ...]:
            return tuple(str(d.get(f, "")) for f in contract.identity_fields)

        row_0217 = {
            "source_recall_id": "76385",
            "press_release_url": (
                "https://www.fda.gov/AnimalVeterinary/NewsEvents/CVMUpdates/ucm542265.htm"
            ),
            "press_release_type": "FDA",
            "press_release_issued_dt": "2017-02-17T00:00:00Z",
        }
        row_0302 = {**row_0217, "press_release_issued_dt": "2017-03-02T00:00:00Z"}

        # The collision pair now resolves to two distinct identity buckets.
        assert identity(row_0217) != identity(row_0302)

        # A genuine byte-duplicate shares both identity and content_hash → collapsible.
        dup = dict(row_0217)
        assert identity(row_0217) == identity(dup)
        assert content_hash(row_0217) == content_hash(dup)


class TestFromContract:
    def test_propagates_oracle_and_inherits_default_flags(self) -> None:
        contract = DEDUP_CONTRACT_BY_SOURCE_NAME["nhtsa"]
        loader = _loader(contract)
        assert loader._identity_fields == contract.identity_fields
        assert loader._hash_exclude_fields == contract.hash_exclude_fields
        # No overrides passed → inherit the contract's incremental defaults.
        assert loader._within_batch_dedup is True
        assert loader._allow_null_identity is True

    def test_fda_incremental_defaults_no_within_batch_dedup(self) -> None:
        loader = _loader(DEDUP_CONTRACT_BY_SOURCE_NAME["fda"])
        assert loader._within_batch_dedup is False  # incremental default

    def test_explicit_override_beats_contract_default(self) -> None:
        """FDA's deep-rescan passes within_batch_dedup=True to collapse productid
        tie-boundary straddles — the override must win over the contract default."""
        loader = _loader(DEDUP_CONTRACT_BY_SOURCE_NAME["fda"], within_batch_dedup=True)
        assert loader._within_batch_dedup is True

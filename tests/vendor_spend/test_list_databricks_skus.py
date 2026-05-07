"""Unit tests for vendor_spend.list_databricks_skus (pure helpers)."""

import pytest

from lucille.vendor_spend.list_databricks_skus import (
    KNOWN_LIST_PRICES_USD_PER_DBU,
    lookup_suggested_price,
    normalize_sku_for_lookup,
)


class TestNormalizeSkuForLookup:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            # Plain SKUs unchanged
            ("PREMIUM_ALL_PURPOSE_COMPUTE", "PREMIUM_ALL_PURPOSE_COMPUTE"),
            ("PREMIUM_JOBS_COMPUTE",        "PREMIUM_JOBS_COMPUTE"),
            # Photon parenthesized form -> underscore form
            ("PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)", "PREMIUM_ALL_PURPOSE_COMPUTE_PHOTON"),
            ("PREMIUM_JOBS_COMPUTE_(PHOTON)",        "PREMIUM_JOBS_COMPUTE_PHOTON"),
            # Region suffixes stripped
            ("PREMIUM_SQL_PRO_COMPUTE_US_EAST_OHIO",            "PREMIUM_SQL_PRO_COMPUTE"),
            ("PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO",     "PREMIUM_SERVERLESS_SQL_COMPUTE"),
            ("PREMIUM_JOBS_SERVERLESS_COMPUTE_US_WEST_OREGON",  "PREMIUM_JOBS_SERVERLESS_COMPUTE"),
            ("PREMIUM_X_EU_WEST_IRELAND",                       "PREMIUM_X"),
            ("PREMIUM_X_AP_SOUTHEAST_SYDNEY",                   "PREMIUM_X"),
            ("PREMIUM_X_CA_CENTRAL_MONTREAL",                   "PREMIUM_X"),
            # Region suffix with no city (just continent_direction)
            ("PREMIUM_X_US_EAST", "PREMIUM_X"),
            # Edge: empty / whitespace
            ("",        ""),
            ("   ",     ""),
        ],
    )
    def test_normalization_table(self, raw, expected):
        assert normalize_sku_for_lookup(raw) == expected

    def test_does_not_strip_non_region_trailing_uppercase(self):
        # We only want to strip continent-prefixed suffixes.
        # Random trailing uppercase like _PHOTON should not be stripped.
        assert normalize_sku_for_lookup("PREMIUM_ALL_PURPOSE_COMPUTE_PHOTON") == (
            "PREMIUM_ALL_PURPOSE_COMPUTE_PHOTON"
        )

    def test_idempotent(self):
        # Normalizing twice should give the same result.
        for sku in [
            "PREMIUM_SQL_PRO_COMPUTE_US_EAST_OHIO",
            "PREMIUM_JOBS_COMPUTE_(PHOTON)",
            "PREMIUM_ALL_PURPOSE_COMPUTE",
        ]:
            once = normalize_sku_for_lookup(sku)
            twice = normalize_sku_for_lookup(once)
            assert once == twice

    def test_combined_photon_and_region(self):
        # Hypothetical (not seen in user data, but should still work)
        assert normalize_sku_for_lookup(
            "PREMIUM_JOBS_COMPUTE_(PHOTON)_US_EAST_OHIO"
        ) == "PREMIUM_JOBS_COMPUTE_PHOTON"


class TestLookupSuggestedPrice:
    def test_exact_match(self):
        assert lookup_suggested_price("PREMIUM_ALL_PURPOSE_COMPUTE") == 0.55

    def test_via_region_suffix_normalization(self):
        assert lookup_suggested_price("PREMIUM_SQL_PRO_COMPUTE_US_EAST_OHIO") == (
            KNOWN_LIST_PRICES_USD_PER_DBU["PREMIUM_SQL_PRO_COMPUTE"]
        )

    def test_via_photon_normalization(self):
        # _(PHOTON) -> _PHOTON, then look up
        assert lookup_suggested_price("PREMIUM_ALL_PURPOSE_COMPUTE_(PHOTON)") == (
            KNOWN_LIST_PRICES_USD_PER_DBU["PREMIUM_ALL_PURPOSE_COMPUTE_PHOTON"]
        )

    def test_via_combined_normalization(self):
        # Region-suffixed serverless SKUs from the user's actual list
        assert lookup_suggested_price(
            "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO"
        ) == 0.70
        assert lookup_suggested_price(
            "PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO"
        ) == 0.35
        assert lookup_suggested_price(
            "PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_OHIO"
        ) == 0.75

    def test_unknown_returns_none(self):
        assert lookup_suggested_price("TOTALLY_MADE_UP_SKU") is None

    def test_empty_returns_none(self):
        assert lookup_suggested_price("") is None

    def test_raw_match_takes_precedence_over_normalized(self):
        # Sanity: if a SKU appears verbatim in the table, we use that value
        # rather than re-normalizing it. (None of our current keys end in a
        # region suffix, so this is a defensive check on lookup ordering.)
        assert lookup_suggested_price("PREMIUM_JOBS_COMPUTE") == (
            KNOWN_LIST_PRICES_USD_PER_DBU["PREMIUM_JOBS_COMPUTE"]
        )

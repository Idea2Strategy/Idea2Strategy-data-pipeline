import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from data_collection.etf_universe import (
    DEFAULT_ETF_UNIVERSE_FILE,
    load_etf_symbols,
)

# The curated-universe CSV header, kept on its own so the fixtures below stay
# inside the 120-column lint limit.
CSV_HEADER = (
    "ticker,name,asset_class,category,benchmark,issuer,structure,"
    "inception_date,leveraged,inverse,enabled,reviewed_at,source_url"
)


class EtfUniverseTests(unittest.TestCase):
    def test_curated_universe_contains_expected_27_etps(self):
        symbols = load_etf_symbols(
            DEFAULT_ETF_UNIVERSE_FILE,
            as_of=datetime(2026, 7, 22, tzinfo=timezone.utc),
        )

        self.assertEqual(len(symbols), 27)
        self.assertTrue(
            {
                "SPY",
                "QQQ",
                "IWM",
                "AGG",
                "SGOV",
                "XLK",
                "XLF",
                "XLV",
                "XLC",
                "GLD",
                "USO",
                "UUP",
                "BITO",
            }.issubset(symbols)
        )

    def test_loader_accepts_explicit_alternative_etp_structures(self):
        content = CSV_HEADER + """
GLD,Gold Trust,alternative_etp,gold,Gold,Issuer,grantor_trust,2004-11-18,false,false,true,2026-07-22,https://example.com/gld
USO,Oil Pool,alternative_etp,oil,Oil,Issuer,commodity_pool,2006-04-10,false,false,true,2026-07-22,https://example.com/uso
"""
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "etps.csv"
            path.write_text(content, encoding="utf-8")

            symbols = load_etf_symbols(
                path,
                as_of=datetime(2026, 7, 22, tzinfo=timezone.utc),
            )

        self.assertEqual(symbols, ["GLD", "USO"])

    def test_loader_excludes_disabled_complex_and_recent_products(self):
        content = CSV_HEADER + """
SPY,Market ETF,equity,market,Index,Issuer,uit,1993-01-22,false,false,true,2026-07-22,https://example.com/spy
TQQQ,Leveraged ETF,equity,leveraged,Index,Issuer,open_end,2010-02-09,true,false,true,2026-07-22,https://example.com/tqqq
SH,Inverse ETF,equity,inverse,Index,Issuer,open_end,2006-06-19,false,true,true,2026-07-22,https://example.com/sh
NEW,Recent ETF,equity,recent,Index,Issuer,open_end,2025-01-01,false,false,true,2026-07-22,https://example.com/new
OFF,Disabled ETF,equity,disabled,Index,Issuer,open_end,2000-01-01,false,false,false,2026-07-22,https://example.com/off
"""
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "etfs.csv"
            path.write_text(content, encoding="utf-8")

            symbols = load_etf_symbols(
                path,
                as_of=datetime(2026, 7, 22, tzinfo=timezone.utc),
            )

        self.assertEqual(symbols, ["SPY"])

    def test_loader_rejects_etn_structure(self):
        content = CSV_HEADER + """
VXX,Volatility Note,alternative,volatility,Index,Issuer,etn,2009-01-30,false,false,true,2026-07-22,https://example.com/vxx
"""
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "etfs.csv"
            path.write_text(content, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "허용되지 않은 상품 구조"):
                load_etf_symbols(
                    path,
                    as_of=datetime(2026, 7, 22, tzinfo=timezone.utc),
                )

    def test_loader_rejects_duplicate_tickers(self):
        content = CSV_HEADER + """
VXX,Volatility ETF,alternative,volatility,Index,Issuer,open_end,2009-01-30,false,false,true,2026-07-22,https://example.com/vxx
VXX,Duplicate Note,alternative,volatility,Index,Issuer,etn,2009-01-30,false,false,true,2026-07-22,https://example.com/vxx
"""
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "etfs.csv"
            path.write_text(content, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "중복"):
                load_etf_symbols(
                    path,
                    as_of=datetime(2026, 7, 22, tzinfo=timezone.utc),
                )


if __name__ == "__main__":
    unittest.main()

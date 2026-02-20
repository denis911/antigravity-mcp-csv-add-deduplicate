import pytest
import pandas as pd
import shutil
from pathlib import Path
import asyncio
from linkedin_prospecting_csv import csv_ops

# Paths to real test data
TEST_DIR = Path(__file__).parent
SMALL_CSV = TEST_DIR / "CSV_1_small.csv"
BIG_CSV = TEST_DIR / "CSV_3_big.csv"

@pytest.fixture
def temp_csv(tmp_path):
    """Creates a temporary copy of the small CSV for testing."""
    temp_file = tmp_path / "test_profiles.csv"
    shutil.copy(SMALL_CSV, temp_file)
    return temp_file

@pytest.mark.asyncio
async def test_get_stats(temp_csv):
    """Test statistics calculation on the initial small CSV."""
    stats = await csv_ops.get_csv_stats(str(temp_csv))
    assert stats["total_profiles"] == 10
    assert stats["avg_score"] > 0
    assert "USA (Seattle)" in stats["location_breakdown"]

@pytest.mark.asyncio
async def test_append_and_deduplicate(temp_csv):
    """Test appending records from the big CSV and ensuring deduplication."""
    # Read records from big CSV
    big_df = pd.read_csv(BIG_CSV)
    profiles_to_add = big_df.to_dict(orient='records')
    
    # Initial count
    initial_stats = await csv_ops.get_csv_stats(str(temp_csv))
    initial_count = initial_stats["total_profiles"]
    
    # Append records
    result = await csv_ops.append_profiles_to_csv(str(temp_csv), profiles_to_add)
    
    # The big CSV contains 55 records, but some might be duplicates of the small one
    # Small CSV has 10 records.
    # Let's check how many were actually added
    final_stats = await csv_ops.get_csv_stats(str(temp_csv))
    assert final_stats["total_profiles"] >= initial_count
    
    # Try appending the same records again to verify deduplication
    result_second = await csv_ops.append_profiles_to_csv(str(temp_csv), profiles_to_add)
    assert result_second["added"] == 0
    assert result_second["skipped_duplicates"] == len(profiles_to_add)
    assert result_second["total_profiles"] == final_stats["total_profiles"]

@pytest.mark.asyncio
async def test_filter_profiles(temp_csv):
    """Test filtering logic."""
    # Filter by score
    high_scorers = await csv_ops.filter_profiles(str(temp_csv), min_score=20)
    assert len(high_scorers) > 0
    for p in high_scorers:
        assert p["v2 Score"] >= 20

    # Filter by location
    canada_profiles = await csv_ops.filter_profiles(str(temp_csv), locations=["Canada"])
    assert len(canada_profiles) > 0
    for p in canada_profiles:
        assert "Canada" in p["Location"]

@pytest.mark.asyncio
async def test_search_profiles(temp_csv):
    """Test full-text search."""
    # Search for 'Meta'
    meta_profiles = await csv_ops.search_profiles(str(temp_csv), search_term="Meta")
    assert len(meta_profiles) > 0
    
    # Search for something non-existent
    none_profiles = await csv_ops.search_profiles(str(temp_csv), search_term="NON_EXISTENT_STUFF_12345")
    assert len(none_profiles) == 0

@pytest.mark.asyncio
async def test_export_segment(temp_csv, tmp_path):
    """Test exporting a segment to a new file."""
    output_file = tmp_path / "exported.csv"
    result = await csv_ops.export_segment(
        source_csv=str(temp_csv),
        output_csv=str(output_file),
        min_score=22,
        columns=["Name", "v2 Score"]
    )
    
    assert result["profiles_exported"] > 0
    assert output_file.exists()
    
    exported_df = pd.read_csv(output_file)
    assert list(exported_df.columns) == ["Name", "v2 Score"]
    assert (exported_df["v2 Score"] >= 22).all()

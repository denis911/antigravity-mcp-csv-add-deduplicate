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
async def test_create_new_csv(tmp_path):
    """Test creating a new CSV with Golden Schema."""
    new_csv = tmp_path / "new_golden.csv"
    result = await csv_ops.create_new_csv(str(new_csv))
    assert result["status"] == "success"
    assert new_csv.exists()
    
    df = pd.read_csv(new_csv)
    assert list(df.columns) == csv_ops.GOLDEN_SCHEMA

@pytest.mark.asyncio
async def test_auto_normalization_on_load(temp_csv):
    """Test that loading an existing V1 CSV auto-normalizes its headers."""
    # First, verify it hasn't been normalized yet (it's a copy of SMALL_CSV)
    df_raw = pd.read_csv(temp_csv)
    assert "v2 Score" in df_raw.columns
    assert "LinkedIn URL" in df_raw.columns
    
    # Trigger normalization via stats
    stats = await csv_ops.get_csv_stats(str(temp_csv))
    assert stats["total_profiles"] == 10
    
    # Reload and check headers
    df_normalized = pd.read_csv(temp_csv)
    assert "match_score" in df_normalized.columns
    assert "linkedin_url" in df_normalized.columns
    assert "full_name" in df_normalized.columns
    assert list(df_normalized.columns[:len(csv_ops.GOLDEN_SCHEMA)]) == csv_ops.GOLDEN_SCHEMA

@pytest.mark.asyncio
async def test_append_with_preview_and_normalization(temp_csv):
    """Test appending records with legacy names and getting a preview."""
    # Records with legacy names
    profiles_to_add = [
        {
            "Name": "New Person 1",
            "LinkedIn URL": "https://linkedin.com/in/newperson1",
            "v2 Score": 25,
            "Location": "Germany"
        },
        {
            "Name": "New Person 2",
            "LinkedIn URL": "https://linkedin.com/in/newperson2",
            "v2 Score": 22
        }
    ]
    
    result = await csv_ops.append_profiles_to_csv(str(temp_csv), profiles_to_add)
    assert result["added"] == 2
    assert "New Person 1" in result["preview"]
    assert "New Person 2" in result["preview"]
    
    # Verify in file
    df = pd.read_csv(temp_csv)
    assert "New Person 1" in df["full_name"].values
    assert 25 in df["match_score"].values

@pytest.mark.asyncio
async def test_filter_profiles_multi_value(temp_csv):
    """Test filtering with multiple values."""
    # First normalize the file
    await csv_ops.get_csv_stats(str(temp_csv))
    
    # Filter by multiple locations
    results = await csv_ops.filter_profiles(str(temp_csv), locations=["USA", "Canada"])
    assert len(results) > 0
    for p in results:
        loc = p["location"].lower()
        assert "usa" in loc or "canada" in loc

@pytest.mark.asyncio
async def test_search_profiles_case_insensitive(temp_csv):
    """Test case-insensitive search."""
    # Normalize
    await csv_ops.get_csv_stats(str(temp_csv))
    
    # Search for 'meta' (lowercase)
    results = await csv_ops.search_profiles(str(temp_csv), search_term="meta")
    assert len(results) > 0
    
    # All results should have 'Meta' in one of the fields (Company is standard)
    found_correct = False
    for p in results:
        if "Meta" in p["company"]:
            found_correct = True
            break
    assert found_correct

@pytest.mark.asyncio
async def test_export_segment_golden_schema(temp_csv, tmp_path):
    """Test exporting a segment ensures golden schema columns."""
    output_file = tmp_path / "exported_v2.csv"
    result = await csv_ops.export_segment(
        source_csv=str(temp_csv),
        output_csv=str(output_file),
        min_score=20,
        columns=["full_name", "match_score"]
    )
    
    assert result["profiles_exported"] > 0
    df = pd.read_csv(output_file)
    assert list(df.columns) == ["full_name", "match_score"]
    assert (df["match_score"] >= 20).all()

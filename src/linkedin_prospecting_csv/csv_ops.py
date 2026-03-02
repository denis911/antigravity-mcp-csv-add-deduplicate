import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
import json
import os
import tempfile

logger = logging.getLogger(__name__)

# Golden Schema Definition
GOLDEN_SCHEMA = [
    "full_name",
    "linkedin_url",
    "headline",
    "company",
    "company_size",
    "location",
    "match_score",
    "match_reason",
    "current_role_mention",
    "found_date",
    "icp_source"
]

RENAME_MAP = {
    "v2_score": "match_score",
    "v2 score": "match_score",
    "name": "full_name",
    "linkedin_profile": "linkedin_url",
    "linkedin url": "linkedin_url",
    "current role mention": "current_role_mention",
    "company size": "company_size",
    "found date": "found_date",
    "icp source": "icp_source",
    "match reason": "match_reason"
}

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize headers to snake_case and map legacy names to Golden Schema.
    """
    # 1. Clean whitespace from headers and convert to snake_case
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    
    # 2. Map specific common variations
    df = df.rename(columns=RENAME_MAP)
    
    # 3. Ensure all golden schema columns exist
    for col in GOLDEN_SCHEMA:
        if col not in df.columns:
            df[col] = ""
            
    # 4. Reorder to match Golden Schema (plus any extra columns)
    extra_cols = [c for c in df.columns if c not in GOLDEN_SCHEMA]
    df = df[GOLDEN_SCHEMA + extra_cols]
    
    return df

def safe_to_csv(df: pd.DataFrame, path: Path):
    """
    Write DataFrame to CSV atomically using a temporary file.
    """
    fd, temp_path = tempfile.mkstemp(suffix=".csv", dir=path.parent)
    os.close(fd)
    try:
        df.to_csv(temp_path, index=False)
        os.replace(temp_path, path)
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise e

async def create_new_csv(csv_path: str, overwrite: bool = False) -> Dict[str, str]:
    """
    Creates a new CSV with the Golden Schema headers.
    """
    path = Path(csv_path).absolute()
    if path.exists() and not overwrite:
        raise FileExistsError(f"File already exists at {path}. Use overwrite=True to replace it.")
    
    df = pd.DataFrame(columns=GOLDEN_SCHEMA)
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_to_csv(df, path)
    
    return {
        "status": "success",
        "message": f"Created new CSV with Golden Schema at {path}",
        "path": str(path)
    }

async def append_profiles_to_csv(
    csv_path: str,
    profiles: List[Dict[str, Any]],
    dedupe_column: str = "linkedin_url"
) -> Dict[str, Any]:
    """
    Append new profiles to CSV with auto-normalization and deduplication.
    """
    path = Path(csv_path).absolute()
    
    # Normalize incoming profiles
    new_df = pd.DataFrame(profiles)
    new_df = normalize_dataframe(new_df)
    
    # Map input dedupe column to normalized name if necessary
    norm_dedupe = dedupe_column.lower().replace(" ", "_").replace("-", "_")
    norm_dedupe = RENAME_MAP.get(norm_dedupe, norm_dedupe)

    existing_df = pd.DataFrame()
    if path.exists():
        try:
            existing_df = pd.read_csv(path)
            existing_df = normalize_dataframe(existing_df)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            logger.error(f"Error reading existing CSV: {e}")
            raise
    else:
        combined_df = new_df

    if norm_dedupe in combined_df.columns:
        combined_df[norm_dedupe] = combined_df[norm_dedupe].astype(str).str.strip()
        combined_df = combined_df.drop_duplicates(subset=[norm_dedupe], keep='first')
    
    final_count = len(combined_df)
    added_df = combined_df.iloc[len(existing_df):] if len(existing_df) < len(combined_df) else pd.DataFrame()
    added_count = len(added_df)
    skipped_count = len(profiles) - added_count
    
    # Extract preview (first 3 names)
    preview = []
    if not added_df.empty:
        preview = added_df["full_name"].head(3).tolist()

    safe_to_csv(combined_df, path)
    
    return {
        "added": int(added_count),
        "skipped_duplicates": int(skipped_count),
        "total_profiles": int(final_count),
        "preview": preview,
        "path": str(path)
    }

def auto_repair_file(path: Path) -> pd.DataFrame:
    """
    Read CSV, normalize it, and if it changed, save it back to disk.
    Returns the normalized DataFrame.
    """
    df_raw = pd.read_csv(path)
    original_cols = df_raw.columns.tolist()
    
    df_norm = normalize_dataframe(df_raw)
    
    # If the file hasn't been normalized yet, or we added missing columns, save it
    if df_norm.columns.tolist() != original_cols:
        logger.info(f"Auto-repairing CSV headers for {path}")
        safe_to_csv(df_norm, path)
        
    return df_norm

async def filter_profiles(
    csv_path: str,
    min_score: Optional[int] = None,
    max_score: Optional[int] = None,
    locations: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    current_role_only: Optional[bool] = None,
    found_after_date: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Filter profiles based on criteria using normalized column names.
    Includes Auto-Repair on load.
    """
    path = Path(csv_path).absolute()
    if not path.exists():
        return []
    
    df = auto_repair_file(path)
    
    # Ensure match_score is numeric
    df["match_score"] = pd.to_numeric(df["match_score"], errors='coerce')
    
    if min_score is not None:
        df = df[df["match_score"] >= min_score]
    if max_score is not None:
        df = df[df["match_score"] <= max_score]
    
    if locations:
        pattern = '|'.join(locations)
        df = df[df["location"].astype(str).str.contains(pattern, case=False, na=False)]
        
    if companies:
        pattern = '|'.join(companies)
        df = df[df["company"].astype(str).str.contains(pattern, case=False, na=False)]
        
    if current_role_only:
        df = df[df["current_role_mention"].astype(str).str.startswith('YES', na=False)]
        
    if found_after_date:
        df["found_date"] = pd.to_datetime(df["found_date"], errors='coerce')
        after_dt = pd.to_datetime(found_after_date)
        df = df[df["found_date"] > after_dt]
        df["found_date"] = df["found_date"].dt.strftime('%Y-%m-%d')
        
    df = df.sort_values(by="match_score", ascending=False)
        
    if limit:
        df = df.head(limit)
        
    return df.fillna("").to_dict(orient='records')

async def get_csv_stats(csv_path: str) -> Dict[str, Any]:
    """
    Get statistics about profiles in CSV with robust error handling.
    Includes Auto-Repair on load.
    """
    path = Path(csv_path).absolute()
    if not path.exists():
        return {"error": "File not found"}
        
    df = auto_repair_file(path)
    total = len(df)
    
    # match_score stats
    df["match_score"] = pd.to_numeric(df["match_score"], errors='coerce')
    avg_score = float(df["match_score"].mean()) if not df["match_score"].isna().all() else 0
    score_dist = {
        "20+": int((df["match_score"] >= 20).sum()),
        "15-19": int(((df["match_score"] >= 15) & (df["match_score"] < 20)).sum()),
        "10-14": int(((df["match_score"] >= 10) & (df["match_score"] < 15)).sum()),
        "<10": int((df["match_score"] < 10).sum())
    }
        
    location_breakdown = df["location"].value_counts().head(10).to_dict()
    location_breakdown = {str(k): int(v) for k, v in location_breakdown.items()}
    
    company_size_breakdown = df["company_size"].value_counts().to_dict()
    company_size_breakdown = {str(k): int(v) for k, v in company_size_breakdown.items()}
    
    found_dates = pd.to_datetime(df["found_date"], errors='coerce')
    date_range = {
        "earliest": str(found_dates.min().date()) if not found_dates.isna().all() else "",
        "latest": str(found_dates.max().date()) if not found_dates.isna().all() else ""
    }
    
    current_role_count = int(df["current_role_mention"].astype(str).str.startswith('YES', na=False).sum())
    
    return {
        "total_profiles": total,
        "avg_score": round(avg_score, 2),
        "score_distribution": score_dist,
        "location_breakdown": location_breakdown,
        "company_size_breakdown": company_size_breakdown,
        "found_date_range": date_range,
        "current_role_count": current_role_count,
        "path": str(path)
    }

async def export_segment(
    source_csv: str,
    output_csv: str,
    min_score: Optional[int] = None,
    locations: Optional[List[str]] = None,
    companies: Optional[List[str]] = None,
    columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Export filtered profiles to new CSV file following Golden Schema.
    """
    profiles = await filter_profiles(
        csv_path=source_csv,
        min_score=min_score,
        locations=locations,
        companies=companies
    )
    
    if not profiles:
        return {"profiles_exported": 0, "output_path": output_csv, "columns_included": []}
        
    df = pd.DataFrame(profiles)
    df = normalize_dataframe(df)
    
    if columns:
        # Normalize requested columns
        norm_cols = [c.lower().replace(" ", "_").replace("-", "_") for c in columns]
        norm_cols = [RENAME_MAP.get(c, c) for c in norm_cols]
        existing_cols = [c for c in norm_cols if c in df.columns]
        df = df[existing_cols]
    
    out_path = Path(output_csv).absolute()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    safe_to_csv(df, out_path)
    
    return {
        "profiles_exported": len(df),
        "output_path": str(out_path),
        "columns_included": df.columns.tolist()
    }

async def search_profiles(
    csv_path: str,
    search_term: str,
    columns: Optional[List[str]] = None,
    case_sensitive: bool = False,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Search for profiles containing search term (case-insensitive by default).
    """
    path = Path(csv_path).absolute()
    if not path.exists():
        return []
        
    df = auto_repair_file(path)
    
    if columns:
        norm_cols = [c.lower().replace(" ", "_").replace("-", "_") for c in columns]
        norm_cols = [RENAME_MAP.get(c, c) for c in norm_cols]
        search_cols = [c for c in norm_cols if c in df.columns]
    else:
        # Default search columns in Golden Schema
        search_cols = ["headline", "company", "match_reason", "current_role_mention", "full_name"]
        search_cols = [c for c in search_cols if c in df.columns]
    
    mask = pd.Series([False] * len(df))
    for col in search_cols:
        mask |= df[col].astype(str).str.contains(search_term, case=case_sensitive, na=False, regex=False)
        
    result_df = df[mask]
    
    # Sort by score
    result_df["match_score"] = pd.to_numeric(result_df["match_score"], errors='coerce')
    result_df = result_df.sort_values(by="match_score", ascending=False)
        
    if limit:
        result_df = result_df.head(limit)
        
    return result_df.fillna("").to_dict(orient="records")

async def deduplicate_csv(
    csv_path: str,
    dedupe_column: str = "linkedin_url",
    keep: str = "first"
) -> Dict[str, Any]:
    """
    Remove duplicates from CSV file using standardized columns.
    """
    path = Path(csv_path).absolute()
    if not path.exists():
        return {"error": "File not found"}
        
    df = pd.read_csv(path)
    df = normalize_dataframe(df)
    original_count = len(df)
    
    norm_dedupe = dedupe_column.lower().replace(" ", "_").replace("-", "_")
    norm_dedupe = RENAME_MAP.get(norm_dedupe, norm_dedupe)
    
    if norm_dedupe in df.columns:
        df[norm_dedupe] = df[norm_dedupe].astype(str).str.strip()
        df = df.drop_duplicates(subset=[norm_dedupe], keep=keep)
        
    final_count = len(df)
    safe_to_csv(df, path)
    
    return {
        "original_count": int(original_count),
        "duplicates_removed": int(original_count - final_count),
        "final_count": int(final_count),
        "path": str(path)
    }

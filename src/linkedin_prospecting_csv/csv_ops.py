import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
import json

logger = logging.getLogger(__name__)

async def append_profiles_to_csv(
    csv_path: str,
    profiles: List[Dict[str, Any]],
    dedupe_column: str = "LinkedIn URL"
) -> Dict[str, int]:
    """
    Append new profiles to CSV, removing duplicates based on dedupe_column.
    """
    path = Path(csv_path)
    new_df = pd.DataFrame(profiles)
    
    existing_df = pd.DataFrame()
    if path.exists():
        try:
            existing_df = pd.read_csv(path)
            columns = existing_df.columns.tolist()
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            logger.error(f"Error reading existing CSV: {e}")
            raise
    else:
        combined_df = new_df
        columns = new_df.columns.tolist()

    if dedupe_column in combined_df.columns:
        combined_df[dedupe_column] = combined_df[dedupe_column].astype(str).str.strip()
        combined_df = combined_df.drop_duplicates(subset=[dedupe_column], keep='first')
    
    final_count = len(combined_df)
    added = final_count - len(existing_df)
    skipped = len(profiles) - added
    
    combined_df = combined_df.reindex(columns=columns)
    combined_df.to_csv(path, index=False)
    
    return {
        "added": int(added),
        "skipped_duplicates": int(skipped),
        "total_profiles": int(final_count)
    }

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
    Filter profiles based on criteria.
    """
    path = Path(csv_path)
    if not path.exists():
        return []
    
    df = pd.read_csv(path)
    
    if 'v2 Score' in df.columns:
        df['v2 Score'] = pd.to_numeric(df['v2 Score'], errors='coerce')
    
    if min_score is not None:
        df = df[df['v2 Score'] >= min_score]
    if max_score is not None:
        df = df[df['v2 Score'] <= max_score]
    
    if locations:
        pattern = '|'.join(locations)
        df = df[df['Location'].str.contains(pattern, case=False, na=False)]
        
    if companies:
        pattern = '|'.join(companies)
        df = df[df['Company'].str.contains(pattern, case=False, na=False)]
        
    if current_role_only:
        df = df[df['CURRENT Role Mention'].str.startswith('YES', na=False)]
        
    if found_after_date:
        df['Found Date'] = pd.to_datetime(df['Found Date'], errors='coerce')
        after_dt = pd.to_datetime(found_after_date)
        df = df[df['Found Date'] > after_dt]
        df['Found Date'] = df['Found Date'].dt.strftime('%Y-%m-%d')
        
    if 'v2 Score' in df.columns:
        df = df.sort_values(by='v2 Score', ascending=False)
        
    if limit:
        df = df.head(limit)
        
    return df.fillna("").to_dict(orient='records')

async def get_csv_stats(csv_path: str) -> Dict[str, Any]:
    """
    Get statistics about profiles in CSV.
    """
    path = Path(csv_path)
    if not path.exists():
        return {"error": "File not found"}
        
    df = pd.read_csv(path)
    total = len(df)
    
    if 'v2 Score' in df.columns:
        df['v2 Score'] = pd.to_numeric(df['v2 Score'], errors='coerce')
        avg_score = float(df['v2 Score'].mean())
        score_dist = {
            "20+": int((df['v2 Score'] >= 20).sum()),
            "15-19": int(((df['v2 Score'] >= 15) & (df['v2 Score'] < 20)).sum()),
            "10-14": int(((df['v2 Score'] >= 10) & (df['v2 Score'] < 15)).sum()),
            "<10": int((df['v2 Score'] < 10).sum())
        }
    else:
        avg_score = 0
        score_dist = {}
        
    location_breakdown = df['Location'].value_counts().head(10).to_dict()
    location_breakdown = {str(k): int(v) for k, v in location_breakdown.items()}
    
    company_size_breakdown = df['Company Size'].value_counts().to_dict()
    company_size_breakdown = {str(k): int(v) for k, v in company_size_breakdown.items()}
    
    found_dates = pd.to_datetime(df['Found Date'], errors='coerce')
    date_range = {
        "earliest": str(found_dates.min().date()) if not found_dates.isna().all() else "",
        "latest": str(found_dates.max().date()) if not found_dates.isna().all() else ""
    }
    
    current_role_count = int(df['CURRENT Role Mention'].str.startswith('YES', na=False).sum())
    
    return {
        "total_profiles": total,
        "avg_score": round(avg_score, 2),
        "score_distribution": score_dist,
        "location_breakdown": location_breakdown,
        "company_size_breakdown": company_size_breakdown,
        "found_date_range": date_range,
        "current_role_count": current_role_count
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
    Export filtered profiles to new CSV file.
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
    if columns:
        existing_cols = [c for c in columns if c in df.columns]
        df = df[existing_cols]
    
    df.to_csv(output_csv, index=False)
    
    return {
        "profiles_exported": len(df),
        "output_path": str(Path(output_csv).absolute()),
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
    Search for profiles containing search term.
    """
    path = Path(csv_path)
    if not path.exists():
        return []
        
    df = pd.read_csv(path)
    
    search_cols = columns if columns else [
        'Headline', 'Company', 'Match Reason', 'CURRENT Role Mention'
    ]
    search_cols = [c for c in search_cols if c in df.columns]
    
    mask = pd.Series([False] * len(df))
    for col in search_cols:
        mask |= df[col].astype(str).str.contains(search_term, case=case_sensitive, na=False, regex=False)
        
    result_df = df[mask]
    
    if 'v2 Score' in result_df.columns:
        result_df['v2 Score'] = pd.to_numeric(result_df['v2 Score'], errors='coerce')
        result_df = result_df.sort_values(by='v2 Score', ascending=False)
        
    if limit:
        result_df = result_df.head(limit)
        
    return result_df.fillna("").to_dict(orient='records')

async def deduplicate_csv(
    csv_path: str,
    dedupe_column: str = "LinkedIn URL",
    keep: str = "first"
) -> Dict[str, int]:
    """
    Remove duplicates from CSV file.
    """
    path = Path(csv_path)
    if not path.exists():
        return {"error": "File not found"}
        
    df = pd.read_csv(path)
    original_count = len(df)
    
    if dedupe_column in df.columns:
        df[dedupe_column] = df[dedupe_column].astype(str).str.strip()
        df = df.drop_duplicates(subset=[dedupe_column], keep=keep)
        
    final_count = len(df)
    df.to_csv(path, index=False)
    
    return {
        "original_count": int(original_count),
        "duplicates_removed": int(original_count - final_count),
        "final_count": int(final_count)
    }

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Any


def validate(df: pd.DataFrame, checks: Dict[str, Any], ref_data: Dict[str, pd.DataFrame] = {}) -> List[Dict[str, Any]]:
    results = []

    for constraint in checks.get("constraints", []):
        ctype = constraint.get("type")
        column = constraint.get("column")
        result = {
            "type": ctype,
            "column": column,
            "status": "PASS",
            "description": "",
        }

        try:
            # 1. Schema Validation
            if ctype == "hasColumnCount":
                expected = constraint["value"]
                actual = len(df.columns)
                if actual != expected:
                    result["status"] = "FAIL"
                result["description"] = f"Expected {expected} columns, got {actual}"

            elif ctype == "hasColumnNames":
                expected = set(constraint["columns"])
                actual = set(df.columns)
                if expected != actual:
                    result["status"] = "FAIL"
                result["description"] = f"Expected columns {expected}, got {actual}"

            elif ctype == "hasDtype":
                expected = constraint["dtype"]
                actual = str(df[column].dtype)
                if expected != actual:
                    result["status"] = "FAIL"
                result["description"] = f"Expected dtype '{expected}', got '{actual}'"

            elif ctype == "isNullable":
                nullable = constraint["nullable"]
                nulls = df[column].isnull().sum()
                if not nullable and nulls > 0:
                    result["status"] = "FAIL"
                result["description"] = f"{nulls} null values found"

            elif ctype == "foreignKeyMatch":
                foreign_df = ref_data.get(constraint["ref_table"])
                ref_col = constraint["ref_column"]
                if foreign_df is not None:
                    invalid = ~df[column].isin(foreign_df[ref_col])
                    count = invalid.sum()
                    if count > 0:
                        result["status"] = "FAIL"
                    result["description"] = f"{count} values not found in reference table"

            # 2. Integrity Checks
            elif ctype == "isUnique":
                dupes = df[column].duplicated().sum()
                if dupes > 0:
                    result["status"] = "FAIL"
                result["description"] = f"{dupes} duplicate values"

            elif ctype == "matchesPattern":
                pattern = re.compile(constraint["pattern"])
                mismatches = ~df[column].dropna().astype(str).apply(lambda x: bool(pattern.fullmatch(x)))
                count = mismatches.sum()
                if count > 0:
                    result["status"] = "FAIL"
                result["description"] = f"{count} values failed pattern match"

            # 3. Quality Checks
            elif ctype == "isComplete":
                missing = df[column].isnull().sum()
                if missing > 0:
                    result["status"] = "FAIL"
                result["description"] = f"{missing} missing values"

            elif ctype == "isWithinRange":
                min_v = constraint["min"]
                max_v = constraint["max"]
                outliers = ((df[column] < min_v) | (df[column] > max_v)).sum()
                if outliers > 0:
                    result["status"] = "FAIL"
                result["description"] = f"{outliers} values out of range [{min_v}, {max_v}]"

            elif ctype == "outlierZScore":
                threshold = constraint.get("threshold", 3)
                z_scores = ((df[column] - df[column].mean()) / df[column].std()).abs()
                count = (z_scores > threshold).sum()
                if count > 0:
                    result["status"] = "FAIL"
                result["description"] = f"{count} outliers with z-score > {threshold}"

            # 4. Business Rules
            elif ctype == "expressionCheck":
                expr = constraint["expression"]
                failed = df.query(f"not ({expr})").shape[0]
                if failed > 0:
                    result["status"] = "FAIL"
                result["description"] = f"{failed} rows failed business rule: {expr}"

            # 5. Statistical Checks
            elif ctype == "statCheck":
                metric = constraint["metric"]
                expected = constraint["value"]
                actual = getattr(df[column], metric)()
                if not np.isclose(actual, expected, atol=constraint.get("tolerance", 0.01)):
                    result["status"] = "FAIL"
                result["description"] = f"{metric}: expected ~{expected}, got {actual}"

            elif ctype == "valueDriftCheck":
                prev_val = constraint["previous_value"]
                current_val = df[column].mean()
                drift = abs(current_val - prev_val)
                if drift > constraint["threshold"]:
                    result["status"] = "FAIL"
                result["description"] = f"Mean drifted by {drift} (threshold={constraint['threshold']})"

            # 6. Timeliness
            elif ctype == "isFresh":
                max_age_days = constraint["max_days"]
                latest = pd.to_datetime(df[column]).max()
                now = pd.Timestamp.now()
                age_days = (now - latest).days
                if age_days > max_age_days:
                    result["status"] = "FAIL"
                result["description"] = f"Data is {age_days} days old (limit={max_age_days})"

            # 7. Completeness
            elif ctype == "percentComplete":
                threshold = constraint["threshold"]
                percent = df[column].notnull().mean() * 100
                if percent < threshold:
                    result["status"] = "FAIL"
                result["description"] = f"{percent:.2f}% complete (threshold={threshold}%)"

            else:
                result["status"] = "UNKNOWN"
                result["description"] = f"Unknown check type: {ctype}"

        except Exception as e:
            result["status"] = "ERROR"
            result["description"] = f"Error: {str(e)}"

        results.append(result)

    return results
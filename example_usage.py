"""
Example usage of PD_TOL decorator for error-tolerant pandas operations.

This decorator automatically skips rows that would cause errors during DataFrame operations,
allowing your code to continue processing even when encountering problematic data.
"""

from PDT import PD_TOL
import pandas as pd
import numpy as np
from datetime import datetime


def create_sample_data():
    """
    Create sample data with intentional errors to demonstrate the decorator's functionality.

    Returns:
        tuple: (error_data, clean_data) - DataFrames with and without problematic rows
    """
    # Data with various types of errors that would normally cause exceptions
    error_data = {
        "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"] * 100,
        "age": [25, 30, 35, 40, -1, 30] * 100,  # Negative age (invalid)
        "department": ["Sales", "Tech", "Marketing", "Tech", 3000, "Tech"] * 100,  # Number instead of string
        "salary": [5000, 8000, 123, 123, "Tech", 3000] * 100,  # String in numeric field
        "hire_date": ['2025/04/29 21:36:50', '2025/04/29 23:31:03', '2025/01/30 00:04:20',
                     '2025/04/30 01:21:39', '2025/04/29 14:17:19', ''] * 100  # Empty date string
    }

    # Clean data without errors for comparison
    clean_data = {
        "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"] * 100,
        "age": [25, 30, 35, 40, 28, 30] * 100,
        "department": ["Sales", "Tech", "Marketing", "Tech", "Tech", "Tech"] * 100,
        "salary": [5000, 8000, 123, 123, 3000, 3000] * 100,
        "hire_date": ['2025/04/29 21:36:50', '2025/04/29 23:31:03', '2025/01/30 00:04:20',
                     '2025/04/30 01:21:39', '2025/04/29 14:17:19', '2025/04/29 14:17:19'] * 100
    }

    return pd.DataFrame(error_data), pd.DataFrame(clean_data)


# Example 1: Simple mathematical operation with error-prone data
@PD_TOL
def calculate_salary_in_thousands(df):
    """
    Convert salary from units to thousands.
    Without the decorator, this would fail on non-numeric salary values.
    """
    df["salary_k"] = df["salary"] / 1000
    return df


# Example 2: Date parsing and filtering operation
@PD_TOL
def filter_recent_hires(df, cutoff_date=datetime(2025, 4, 1)):
    """
    Filter employees hired after a specific date.
    Without the decorator, this would fail on malformed date strings.
    """
    def parse_and_filter(row):
        date_str = row['hire_date']
        if not date_str:  # Handle empty strings
            return False
        date_obj = datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")
        return date_obj > cutoff_date

    filtered_df = df[df.apply(parse_and_filter, axis=1)]
    return filtered_df


# Example 3: Complex data processing with multiple potential failure points
@PD_TOL
def process_employee_data(df):
    """
    Perform multiple operations that could fail on problematic data:
    1. Calculate bonus based on salary
    2. Create age categories
    3. Format department names
    """
    # Calculate bonus (10% of salary) - fails if salary is not numeric
    df["bonus"] = df["salary"] * 0.1

    # Create age categories - fails if age is negative or non-numeric
    df["age_category"] = df["age"].apply(lambda x: "Senior" if x >= 35 else "Junior")

    # Uppercase department names - fails if department is not string
    df["department_upper"] = df["department"].str.upper()

    return df


# Example 4: Global DataFrame modification (without return)
df_global = None

@PD_TOL
def modify_global_dataframe():
    """
    Modify a global DataFrame in place.
    Demonstrates decorator usage without return value.
    """
    global df_global
    df_global["adjusted_salary"] = df_global["salary"] * 1.05  # 5% raise


def demonstrate_decorator():
    """
    Demonstrate the PD_TOL decorator with various scenarios.
    """
    print("=== PD_TOL Decorator Demonstration ===\n")

    # Create sample data
    error_df, clean_df = create_sample_data()

    print("Original data shape:", error_df.shape)
    print("Data contains intentional errors in some rows\n")

    # Example 1: Mathematical operations
    print("1. Mathematical Operations (Salary Conversion)")
    print("-" * 50)

    result_df = calculate_salary_in_thousands(error_df.copy())
    print(f"Successfully processed {len(result_df)} rows")
    print("Sample results:")
    print(result_df[["name", "salary", "salary_k"]].head())
    print()

    # Example 2: Date filtering
    print("2. Date Parsing and Filtering")
    print("-" * 50)

    filtered_df = filter_recent_hires(error_df.copy())
    print(f"Filtered to {len(filtered_df)} recent hires")
    print("Sample results:")
    print(filtered_df[["name", "hire_date"]].head())
    print()

    # Example 3: Complex processing
    print("3. Complex Data Processing")
    print("-" * 50)

    processed_df = process_employee_data(error_df.copy())
    print(f"Successfully processed {len(processed_df)} rows")
    print("Sample results:")
    print(processed_df[["name", "salary", "bonus", "age_category", "department_upper"]].head())
    print()

    # Example 4: Global DataFrame modification
    print("4. Global DataFrame Modification")
    print("-" * 50)

    global df_global
    df_global = error_df.copy()
    original_cols = list(df_global.columns)
    print(df_global)

    modify_global_dataframe()
    new_cols = list(df_global.columns)

    print(f"Added columns: {set(new_cols) - set(original_cols)}")
    print("Sample results:")
    print(df_global[["name", "salary", "adjusted_salary"]].head())
    print()


    print("\n=== Demonstration Complete ===")


if __name__ == "__main__":
    demonstrate_decorator()

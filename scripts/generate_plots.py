#!/usr/bin/env python3
"""
Post-pipeline visualization generator for Nedbank Data Engineering Challenge.

This script reads the Delta tables (Parquet files) produced by the pipeline
and generates automated visualizations to verify data quality and pipeline correctness.

Usage: python scripts/generate_plots.py
"""

import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set up clean matplotlib/seaborn styling
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 12
plt.rcParams['axes.titlesize'] = 16
plt.rcParams['axes.labelsize'] = 14

def get_table_path(layer, table_name):
    """Get the path to a Delta table's Parquet files."""
    return f"test_data/output/{layer}/{table_name}/*.parquet"

def read_delta_table(con, layer, table_name):
    """Read a Delta table (Parquet files) into a DuckDB connection."""
    path = get_table_path(layer, table_name)
    if not os.path.exists(path.replace("*.parquet", "")):
        print(f"Warning: {path} not found, skipping {table_name}")
        return None

    try:
        # Create unique table name to avoid conflicts across layers
        unique_table_name = f"{layer}_{table_name}"
        # Drop table if it exists from previous runs
        con.execute(f"DROP TABLE IF EXISTS {unique_table_name}")
        # Read all parquet files in the directory
        con.execute(f"CREATE TABLE {unique_table_name} AS SELECT * FROM read_parquet('{path}')")
        return unique_table_name
    except Exception as e:
        print(f"Error reading {table_name}: {e}")
        return None

def generate_bronze_row_counts(con):
    """Chart a: Bronze row counts per source."""
    print("Generating bronze row counts chart...")

    sources = ['accounts', 'customers', 'transactions']
    counts = []

    for source in sources:
        table = read_delta_table(con, 'bronze', source)
        if table:
            result = con.execute(f"SELECT COUNT(*) as count FROM {table}").fetchone()
            counts.append(result[0])
        else:
            counts.append(0)

    plt.figure(figsize=(10, 6))
    bars = plt.bar(sources, counts, color=['#2E86AB', '#A23B72', '#F18F01'])
    plt.title('Bronze Layer: Row Counts by Source', pad=20, fontweight='bold')
    plt.xlabel('Data Source')
    plt.ylabel('Row Count')

    # Add value labels on bars
    for bar, count in zip(bars, counts):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(counts)*0.01,
                f'{count:,}', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig('docs/bronze_row_counts.png', dpi=300, bbox_inches='tight')
    plt.close()

def generate_silver_dq_distribution(con):
    """Chart b: Silver DQ flag distribution."""
    print("Generating silver DQ flag distribution chart...")

    table = read_delta_table(con, 'silver', 'transactions')
    if not table:
        print("Skipping DQ distribution chart - no silver transactions found")
        return

    # Check if dq_flag column exists and has non-null values
    try:
        result = con.execute(f"""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN dq_flag IS NOT NULL THEN 1 END) as flagged
            FROM {table}
        """).fetchone()

        if result[1] == 0:  # No DQ flags
            print("Skipping DQ distribution chart - no DQ flags found in data")
            return

        # Get DQ flag distribution
        dq_data = con.execute(f"""
            SELECT dq_flag, COUNT(*) as count
            FROM {table}
            WHERE dq_flag IS NOT NULL
            GROUP BY dq_flag
            ORDER BY count DESC
        """).fetchdf()

        plt.figure(figsize=(12, 8))
        colors = sns.color_palette("Set2", n_colors=len(dq_data))

        bars = plt.bar(dq_data['dq_flag'], dq_data['count'], color=colors)
        plt.title('Silver Layer: Data Quality Flag Distribution', pad=20, fontweight='bold')
        plt.xlabel('DQ Flag Type')
        plt.ylabel('Record Count')
        plt.xticks(rotation=45, ha='right')

        # Add value labels
        for bar, count in zip(bars, dq_data['count']):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(dq_data['count'])*0.01,
                    f'{count:,}', ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig('docs/silver_dq_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

    except Exception as e:
        print(f"Error generating DQ distribution chart: {e}")

def generate_gold_transaction_volume(con):
    """Chart c: Gold transaction volume by type."""
    print("Generating gold transaction volume by type chart...")

    table = read_delta_table(con, 'gold', 'fact_transactions')
    if not table:
        print("Skipping transaction volume chart - no fact_transactions found")
        return

    try:
        tx_data = con.execute(f"""
            SELECT transaction_type, COUNT(*) as count
            FROM {table}
            GROUP BY transaction_type
            ORDER BY count DESC
        """).fetchdf()

        plt.figure(figsize=(12, 8))
        colors = sns.color_palette("viridis", n_colors=len(tx_data))

        bars = plt.bar(tx_data['transaction_type'], tx_data['count'], color=colors)
        plt.title('Gold Layer: Transaction Volume by Type', pad=20, fontweight='bold')
        plt.xlabel('Transaction Type')
        plt.ylabel('Transaction Count')

        # Add value labels
        for bar, count in zip(bars, tx_data['count']):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(tx_data['count'])*0.01,
                    f'{count:,}', ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig('docs/gold_transaction_volume.png', dpi=300, bbox_inches='tight')
        plt.close()

    except Exception as e:
        print(f"Error generating transaction volume chart: {e}")

def generate_gold_age_band_distribution(con):
    """Chart d: Gold customer age band distribution."""
    print("Generating gold customer age band distribution chart...")

    table = read_delta_table(con, 'gold', 'dim_customers')
    if not table:
        print("Skipping age band distribution chart - no dim_customers found")
        return

    try:
        age_data = con.execute(f"""
            SELECT age_band, COUNT(*) as count
            FROM {table}
            GROUP BY age_band
            ORDER BY age_band
        """).fetchdf()

        plt.figure(figsize=(12, 8))
        colors = sns.color_palette("coolwarm", n_colors=len(age_data))

        bars = plt.bar(age_data['age_band'], age_data['count'], color=colors)
        plt.title('Gold Layer: Customer Age Band Distribution', pad=20, fontweight='bold')
        plt.xlabel('Age Band')
        plt.ylabel('Customer Count')

        # Add value labels
        for bar, count in zip(bars, age_data['count']):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(age_data['count'])*0.01,
                    f'{count:,}', ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig('docs/gold_age_band_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

    except Exception as e:
        print(f"Error generating age band distribution chart: {e}")

def generate_gold_transaction_by_province(con):
    """Chart e: Gold transaction count by province (join fact_transactions with dim_customers)."""
    print("Generating gold transaction count by province chart...")

    fact_table = read_delta_table(con, 'gold', 'fact_transactions')
    dim_table = read_delta_table(con, 'gold', 'dim_customers')

    if not fact_table or not dim_table:
        print("Skipping transaction by province chart - missing gold tables")
        return

    try:
        province_data = con.execute(f"""
            SELECT dc.province, COUNT(*) as transaction_count
            FROM {fact_table} ft
            JOIN {dim_table} dc ON ft.customer_sk = dc.customer_sk
            GROUP BY dc.province
            ORDER BY transaction_count DESC
        """).fetchdf()

        plt.figure(figsize=(14, 8))
        colors = sns.color_palette("Set3", n_colors=len(province_data))

        bars = plt.bar(province_data['province'], province_data['transaction_count'], color=colors)
        plt.title('Gold Layer: Transaction Count by Customer Province', pad=20, fontweight='bold')
        plt.xlabel('Province')
        plt.ylabel('Transaction Count')
        plt.xticks(rotation=45, ha='right')

        # Add value labels
        for bar, count in zip(bars, province_data['transaction_count']):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(province_data['transaction_count'])*0.01,
                    f'{count:,}', ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        plt.savefig('docs/gold_transaction_by_province.png', dpi=300, bbox_inches='tight')
        plt.close()

    except Exception as e:
        print(f"Error generating transaction by province chart: {e}")

def main():
    """Main function to generate all visualizations."""
    print("🚀 Starting post-pipeline visualization generation...")
    print("=" * 60)

    # Ensure docs directory exists
    os.makedirs('docs', exist_ok=True)

    # Initialize DuckDB connection
    con = duckdb.connect(database=':memory:')

    try:
        # Generate all charts
        generate_bronze_row_counts(con)
        generate_silver_dq_distribution(con)
        generate_gold_transaction_volume(con)
        generate_gold_age_band_distribution(con)
        generate_gold_transaction_by_province(con)

        print("=" * 60)
        print("✅ All visualizations generated successfully!")
        print("📊 Charts saved to docs/ directory:")
        print("   - docs/bronze_row_counts.png")
        print("   - docs/silver_dq_distribution.png")
        print("   - docs/gold_transaction_volume.png")
        print("   - docs/gold_age_band_distribution.png")
        print("   - docs/gold_transaction_by_province.png")

    except Exception as e:
        print(f"❌ Error during visualization generation: {e}")
        return 1

    finally:
        con.close()

    return 0

if __name__ == "__main__":
    exit(main())
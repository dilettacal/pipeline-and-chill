#!/usr/bin/env python3
"""
Explore and visualize curated NYC Taxi data.

This script provides comprehensive analysis and visualization of the curated
parquet files created in Phase 2.

Usage:
    python scripts/explore_curated_data.py
    python scripts/explore_curated_data.py --month 2025-01
    python scripts/explore_curated_data.py --month 2025-05 --save-plots
"""

import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd

# Try to import plotting libraries (optional)
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOTTING_AVAILABLE = True
    # Set style for better-looking plots
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)
except ImportError:
    PLOTTING_AVAILABLE = False
    print("⚠️  matplotlib/seaborn not installed. Install with: pip install matplotlib seaborn")
    print("   Running in statistics-only mode.\n")


def load_month(data_root: Path, year: int, month: int) -> pd.DataFrame:
    """Load curated data for a specific month."""
    path = data_root / "curated" / "yellow" / str(year) / f"{month:02d}" / "trips_clean.parquet"
    
    if not path.exists():
        raise FileNotFoundError(f"Curated file not found: {path}")
    
    print(f"📂 Loading: {path.name}")
    df = pd.read_parquet(path)
    print(f"   ✓ Loaded {len(df):,} rows")
    return df


def print_summary_stats(df: pd.DataFrame, month_str: str):
    """Print comprehensive summary statistics."""
    print("\n" + "=" * 80)
    print(f"📊 SUMMARY STATISTICS - {month_str}")
    print("=" * 80)
    
    # Basic info
    print(f"\n📋 Dataset Info:")
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Memory: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    # Time range
    print(f"\n📅 Time Range:")
    print(f"   First pickup: {df['pickup_ts'].min()}")
    print(f"   Last pickup: {df['pickup_ts'].max()}")
    print(f"   Span: {(df['pickup_ts'].max() - df['pickup_ts'].min()).days} days")
    
    # Trip statistics
    print(f"\n🚖 Trip Statistics:")
    print(f"   Total trips: {len(df):,}")
    print(f"   Avg distance: {df['distance_km'].mean():.2f} km")
    print(f"   Avg duration: {df['duration_min'].mean():.2f} min")
    print(f"   Avg speed: {df['avg_speed_kmh'].mean():.2f} km/h")
    print(f"   Avg fare: ${df['fare_amount'].mean():.2f}")
    print(f"   Avg tip: ${df['tip_amount'].mean():.2f}")
    
    # Payment types
    print(f"\n💳 Payment Types:")
    payment_counts = df['payment_type'].value_counts()
    for payment_type, count in payment_counts.head().items():
        pct = count / len(df) * 100
        print(f"   Type {payment_type}: {count:,} ({pct:.1f}%)")
    
    # Passenger counts
    print(f"\n👥 Passenger Distribution:")
    passenger_counts = df['passenger_count'].value_counts().sort_index()
    for passengers, count in passenger_counts.head().items():
        pct = count / len(df) * 100
        print(f"   {passengers:.0f} passenger(s): {count:,} ({pct:.1f}%)")
    
    # Top zones
    print(f"\n📍 Top 5 Pickup Zones:")
    top_zones = df['pu_zone_id'].value_counts().head()
    for zone_id, count in top_zones.items():
        pct = count / len(df) * 100
        print(f"   Zone {zone_id}: {count:,} trips ({pct:.1f}%)")
    
    # Quantiles for key metrics
    print(f"\n📈 Quantiles (Distance, Duration, Speed, Fare):")
    quantiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    print(f"   {'Quantile':<10} {'Distance (km)':<15} {'Duration (min)':<15} {'Speed (km/h)':<15} {'Fare ($)':<10}")
    print(f"   {'-'*70}")
    for q in quantiles:
        dist = df['distance_km'].quantile(q)
        dur = df['duration_min'].quantile(q)
        speed = df['avg_speed_kmh'].quantile(q)
        fare = df['fare_amount'].quantile(q)
        print(f"   {q:<10.2f} {dist:<15.2f} {dur:<15.2f} {speed:<15.2f} {fare:<10.2f}")


def plot_distributions(df: pd.DataFrame, month_str: str, save: bool = False):
    """Create distribution plots for key metrics."""
    if not PLOTTING_AVAILABLE:
        print("\n⚠️  Plotting skipped (matplotlib not installed)")
        return
    
    print("\n📊 Creating distribution plots...")
    
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    fig.suptitle(f'Distribution Analysis - {month_str}', fontsize=16, fontweight='bold')
    
    # Distance distribution (capped at 50km for visibility)
    df_viz = df[df['distance_km'] <= 50].copy()
    axes[0, 0].hist(df_viz['distance_km'], bins=50, edgecolor='black', alpha=0.7)
    axes[0, 0].set_xlabel('Distance (km)')
    axes[0, 0].set_ylabel('Frequency')
    axes[0, 0].set_title(f'Trip Distance Distribution (≤50km)\nn={len(df_viz):,}')
    axes[0, 0].axvline(df['distance_km'].median(), color='red', linestyle='--', label=f'Median: {df["distance_km"].median():.1f}km')
    axes[0, 0].legend()
    
    # Duration distribution (capped at 60 min for visibility)
    df_viz = df[df['duration_min'] <= 60].copy()
    axes[0, 1].hist(df_viz['duration_min'], bins=50, edgecolor='black', alpha=0.7, color='orange')
    axes[0, 1].set_xlabel('Duration (minutes)')
    axes[0, 1].set_ylabel('Frequency')
    axes[0, 1].set_title(f'Trip Duration Distribution (≤60min)\nn={len(df_viz):,}')
    axes[0, 1].axvline(df['duration_min'].median(), color='red', linestyle='--', label=f'Median: {df["duration_min"].median():.1f}min')
    axes[0, 1].legend()
    
    # Speed distribution (capped at 100 km/h for visibility)
    df_viz = df[df['avg_speed_kmh'] <= 100].copy()
    axes[0, 2].hist(df_viz['avg_speed_kmh'], bins=50, edgecolor='black', alpha=0.7, color='green')
    axes[0, 2].set_xlabel('Speed (km/h)')
    axes[0, 2].set_ylabel('Frequency')
    axes[0, 2].set_title(f'Average Speed Distribution (≤100km/h)\nn={len(df_viz):,}')
    axes[0, 2].axvline(df['avg_speed_kmh'].median(), color='red', linestyle='--', label=f'Median: {df["avg_speed_kmh"].median():.1f}km/h')
    axes[0, 2].legend()
    
    # Fare distribution (capped at $100 for visibility)
    df_viz = df[df['fare_amount'] <= 100].copy()
    axes[1, 0].hist(df_viz['fare_amount'], bins=50, edgecolor='black', alpha=0.7, color='purple')
    axes[1, 0].set_xlabel('Fare Amount ($)')
    axes[1, 0].set_ylabel('Frequency')
    axes[1, 0].set_title(f'Fare Distribution (≤$100)\nn={len(df_viz):,}')
    axes[1, 0].axvline(df['fare_amount'].median(), color='red', linestyle='--', label=f'Median: ${df["fare_amount"].median():.1f}')
    axes[1, 0].legend()
    
    # Passenger count
    passenger_counts = df['passenger_count'].value_counts().sort_index()
    axes[1, 1].bar(passenger_counts.index, passenger_counts.values, edgecolor='black', alpha=0.7, color='teal')
    axes[1, 1].set_xlabel('Passenger Count')
    axes[1, 1].set_ylabel('Frequency')
    axes[1, 1].set_title('Passenger Distribution')
    axes[1, 1].set_xticks(passenger_counts.index)
    
    # Hourly pattern
    df['hour'] = df['pickup_ts'].dt.hour
    hourly = df.groupby('hour').size()
    axes[1, 2].plot(hourly.index, hourly.values, marker='o', linewidth=2, markersize=5, color='darkblue')
    axes[1, 2].set_xlabel('Hour of Day')
    axes[1, 2].set_ylabel('Number of Trips')
    axes[1, 2].set_title('Trips by Hour of Day')
    axes[1, 2].set_xticks(range(0, 24, 2))
    axes[1, 2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if save:
        output_path = Path(f'data/curated/yellow/analysis_{month_str}_distributions.png')
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"   ✓ Saved to: {output_path}")
    else:
        plt.show()


def plot_temporal_patterns(df: pd.DataFrame, month_str: str, save: bool = False):
    """Create temporal analysis plots."""
    if not PLOTTING_AVAILABLE:
        print("\n⚠️  Plotting skipped (matplotlib not installed)")
        return
    
    print("\n📅 Creating temporal pattern plots...")
    
    # Prepare data
    df['date'] = df['pickup_ts'].dt.date
    df['hour'] = df['pickup_ts'].dt.hour
    df['day_of_week'] = df['pickup_ts'].dt.day_name()
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'Temporal Patterns - {month_str}', fontsize=16, fontweight='bold')
    
    # Daily trip volume
    daily = df.groupby('date').size()
    axes[0, 0].plot(daily.index, daily.values, linewidth=2, color='steelblue')
    axes[0, 0].set_xlabel('Date')
    axes[0, 0].set_ylabel('Number of Trips')
    axes[0, 0].set_title('Daily Trip Volume')
    axes[0, 0].tick_params(axis='x', rotation=45)
    axes[0, 0].grid(True, alpha=0.3)
    
    # Hourly heatmap by day of week
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data = df.groupby(['day_of_week', 'hour']).size().unstack(fill_value=0)
    heatmap_data = heatmap_data.reindex(day_order)
    
    sns.heatmap(heatmap_data, ax=axes[0, 1], cmap='YlOrRd', cbar_kws={'label': 'Number of Trips'})
    axes[0, 1].set_xlabel('Hour of Day')
    axes[0, 1].set_ylabel('Day of Week')
    axes[0, 1].set_title('Trip Volume Heatmap')
    
    # Average fare by hour
    hourly_fare = df.groupby('hour')['fare_amount'].mean()
    axes[1, 0].bar(hourly_fare.index, hourly_fare.values, edgecolor='black', alpha=0.7, color='gold')
    axes[1, 0].set_xlabel('Hour of Day')
    axes[1, 0].set_ylabel('Average Fare ($)')
    axes[1, 0].set_title('Average Fare by Hour')
    axes[1, 0].set_xticks(range(0, 24, 2))
    axes[1, 0].grid(True, alpha=0.3, axis='y')
    
    # Average speed by hour
    hourly_speed = df.groupby('hour')['avg_speed_kmh'].mean()
    axes[1, 1].plot(hourly_speed.index, hourly_speed.values, marker='o', linewidth=2, markersize=5, color='darkgreen')
    axes[1, 1].set_xlabel('Hour of Day')
    axes[1, 1].set_ylabel('Average Speed (km/h)')
    axes[1, 1].set_title('Average Speed by Hour')
    axes[1, 1].set_xticks(range(0, 24, 2))
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if save:
        output_path = Path(f'data/curated/yellow/analysis_{month_str}_temporal.png')
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"   ✓ Saved to: {output_path}")
    else:
        plt.show()


def show_sample_data(df: pd.DataFrame, n: int = 10):
    """Display sample rows from the dataset."""
    print("\n" + "=" * 80)
    print(f"📋 SAMPLE DATA (First {n} rows)")
    print("=" * 80)
    
    # Select key columns for display
    display_cols = [
        'pickup_ts', 'dropoff_ts', 'pu_zone_id', 'do_zone_id',
        'distance_km', 'duration_min', 'avg_speed_kmh',
        'fare_amount', 'tip_amount', 'total_amount', 'passenger_count'
    ]
    
    print(df[display_cols].head(n).to_string(index=False))


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Explore and visualize curated NYC Taxi data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Explore all months with summary
  python scripts/explore_curated_data.py
  
  # Explore specific month with plots
  python scripts/explore_curated_data.py --month 2025-05
  
  # Save plots to files
  python scripts/explore_curated_data.py --month 2025-05 --save-plots
        """
    )
    
    parser.add_argument(
        '--month',
        type=str,
        help='Month to explore in YYYY-MM format (default: all months summary)',
    )
    
    parser.add_argument(
        '--data-root',
        type=Path,
        default=Path.cwd() / 'data',
        help='Root data directory (default: ./data)',
    )
    
    parser.add_argument(
        '--save-plots',
        action='store_true',
        help='Save plots to files instead of displaying',
    )
    
    parser.add_argument(
        '--no-plots',
        action='store_true',
        help='Skip plotting (statistics only)',
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("🔍 NYC TAXI DATA EXPLORER - Curated Data Analysis")
    print("=" * 80)
    
    if args.month:
        # Explore specific month
        try:
            dt = datetime.strptime(args.month, "%Y-%m")
            year, month = dt.year, dt.month
        except ValueError:
            print(f"❌ Invalid month format: {args.month}. Expected YYYY-MM")
            return 1
        
        df = load_month(args.data_root, year, month)
        print_summary_stats(df, args.month)
        show_sample_data(df)
        
        if not args.no_plots:
            plot_distributions(df, args.month, args.save_plots)
            plot_temporal_patterns(df, args.month, args.save_plots)
        
    else:
        # Show summary for all months
        print("\n📊 Summary across all months:")
        print("-" * 80)
        
        total_rows = 0
        for month in range(1, 9):
            try:
                df = load_month(args.data_root, 2025, month)
                total_rows += len(df)
                
                print(f"\n2025-{month:02d}:")
                print(f"   Rows: {len(df):,}")
                print(f"   Avg distance: {df['distance_km'].mean():.2f} km")
                print(f"   Avg duration: {df['duration_min'].mean():.2f} min")
                print(f"   Avg fare: ${df['fare_amount'].mean():.2f}")
                
            except FileNotFoundError:
                print(f"\n2025-{month:02d}: ❌ File not found")
        
        print("\n" + "=" * 80)
        print(f"Total rows across all months: {total_rows:,}")
        print("\n💡 Tip: Use --month YYYY-MM to explore a specific month with detailed plots")
    
    print("\n✅ Analysis complete!")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())


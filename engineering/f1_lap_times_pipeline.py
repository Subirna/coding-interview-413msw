# F1 Lap Times Analysis Pipeline

import pandas as pd
import json
import logging
from typing import Dict, List, Tuple
from pathlib import Path


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class F1LapTimesProcessor:
    def __init__(self, input_file: str):
        self.input_file = input_file
        self.data = None
        self.results = None
        
    def validate_data(self, df: pd.DataFrame) -> bool:
        
        # Check required columns exist
        required_columns = ['Driver', 'Time']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing required columns. Expected: {required_columns}")
        
        # Check for empty dataframe
        if df.empty:
            raise ValueError("Input file is empty")
        
        # Check data types
        if not pd.api.types.is_numeric_dtype(df['Time']):
            raise ValueError("Time column must contain numeric values")
        
        # Check for negative times
        if (df['Time'] < 0).any():
            raise ValueError("Time values cannot be negative")
        
        logger.info(f"Data validation passed: {len(df)} records, {df['Driver'].nunique()} unique drivers")
        return True
    
    def extract(self) -> pd.DataFrame:
        try:
            logger.info(f"Reading data from {self.input_file}")
            df = pd.read_csv(self.input_file)
            self.validate_data(df)
            self.data = df
            return df
        except FileNotFoundError:
            logger.error(f"File not found: {self.input_file}")
            raise
        except Exception as e:
            logger.error(f"Error reading file: {str(e)}")
            raise
    
    def transform(self) -> pd.DataFrame:
        if self.data is None:
            raise ValueError("No data loaded. Call extract() first.")
        
        logger.info("Calculating average lap times per driver")
        
        # Group by driver and calculate statistics
        driver_stats = self.data.groupby('Driver').agg(
            average_time=('Time', 'mean'),
            fastest_time=('Time', 'min'),
            lap_count=('Time', 'count')
        ).reset_index()
        
        # Sort by average time (ascending = fastest first)
        driver_stats = driver_stats.sort_values('average_time', ascending=True)
        
        # Add ranking
        driver_stats['rank'] = range(1, len(driver_stats) + 1)
        
        # Round times to 3 decimal places for readability
        driver_stats['average_time'] = driver_stats['average_time'].round(3)
        driver_stats['fastest_time'] = driver_stats['fastest_time'].round(3)
        
        logger.info(f"Transformation complete. Top driver: {driver_stats.iloc[0]['Driver']} "
                   f"with average time {driver_stats.iloc[0]['average_time']}")
        
        self.results = driver_stats
        return driver_stats
    
    def get_top_n(self, n: int = 3) -> pd.DataFrame:
        if self.results is None:
            raise ValueError("No results available. Call transform() first.")
        
        top_n = self.results.head(n)
        logger.info(f"Top {n} drivers retrieved")
        return top_n
    
    def load_csv(self, output_file: str, top_n: int = 3) -> None:
        top_drivers = self.get_top_n(top_n)
        top_drivers.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
    
    def load_json(self, output_file: str, top_n: int = 3) -> None:
        top_drivers = self.get_top_n(top_n)
        
        # Convert to dictionary format
        result_dict = {
            'top_drivers': top_drivers.to_dict('records'),
            'total_drivers_analyzed': len(self.results),
            'total_laps_analyzed': int(self.data['Driver'].count())
        }
        
        with open(output_file, 'w') as f:
            json.dump(result_dict, f, indent=2)
        
        logger.info(f"Results saved to {output_file}")
    
    def print_summary(self, top_n: int = 3) -> None:
        top_drivers = self.get_top_n(top_n)
        
        print("\n" + "="*70)
        print(f"F1 LAP TIMES ANALYSIS - TOP {top_n} DRIVERS")
        print("="*70)
        print(f"{'Rank':<6} {'Driver':<20} {'Fastest':<12} {'Average':<12} {'Laps':<8}")
        print("-"*70)
        
        for _, row in top_drivers.iterrows():
            print(f"{int(row['rank']):<6} {row['Driver']:<20} "
                  f"{row['fastest_time']:<12.3f} {row['average_time']:<12.3f} "
                  f"{int(row['lap_count']):<8}")
        
        print("="*70 + "\n")
    
    def run_pipeline(self, output_csv: str = 'top_drivers.csv', 
                     output_json: str = 'top_drivers.json',
                     top_n: int = 3) -> None:
        try:
            logger.info("Starting F1 Lap Times ETL Pipeline")
            
            # Extract
            self.extract()
            
            # Transform
            self.transform()
            
            # Load
            self.load_csv(output_csv, top_n)
            self.load_json(output_json, top_n)
            
            # Display summary
            self.print_summary(top_n)
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


def main():
    # Configuration
    INPUT_FILE = 'f1_lap_times.csv'
    OUTPUT_CSV = 'top_3_drivers.csv'
    OUTPUT_JSON = 'top_3_drivers.json'
    TOP_N = 3
    
    # Initialize and run pipeline
    processor = F1LapTimesProcessor(INPUT_FILE)
    processor.run_pipeline(
        output_csv=OUTPUT_CSV,
        output_json=OUTPUT_JSON,
        top_n=TOP_N
    )


if __name__ == "__main__":
    main()
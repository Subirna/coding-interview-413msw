# Sample F1 Lap Times Data Generator

import pandas as pd
import random
from typing import List, Tuple


def generate_lap_times(driver_name: str, 
                       base_time: float, 
                       num_laps: int = 5,
                       variance: float = 0.5) -> List[Tuple[str, float]]:

    lap_times = []
    for _ in range(num_laps):
        # Add random variance to base time
        time = base_time + random.uniform(-variance, variance)
        # Ensure time is positive and round to 2 decimal places
        time = round(max(time, 1.0), 2)
        lap_times.append((driver_name, time))
    return lap_times


def create_f1_sample_data(output_file: str = 'f1_lap_times.csv') -> None:
    drivers = [
        ('Hamilton', 4.56),
        ('Verstappen', 4.52),
        ('Leclerc', 4.58),
        ('Perez', 4.61),
        ('Sainz', 4.59),
        ('Russell', 4.57),
        ('Norris', 4.63),
        ('Alonso', 4.65),
        ('Ocon', 4.68),
        ('Gasly', 4.67),
        ('Piastri', 4.64),
        ('Stroll', 4.70),
        ('Tsunoda', 4.69),
        ('Hulkenberg', 4.71),
        ('Ricciardo', 4.66)
    ]
    
    # Generate lap times for each driver
    all_lap_times = []
    for driver_name, base_time in drivers:
        # Each driver gets between 3 and 8 laps
        num_laps = random.randint(3, 8)
        lap_times = generate_lap_times(driver_name, base_time, num_laps)
        all_lap_times.extend(lap_times)
    
    # Shuffle to make data more realistic (not grouped by driver)
    random.shuffle(all_lap_times)
    
    # Create DataFrame
    df = pd.DataFrame(all_lap_times, columns=['Driver', 'Time'])
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    
    print(f"Sample data generated successfully!")
    print(f"File: {output_file}")
    print(f"Total records: {len(df)}")
    print(f"Unique drivers: {df['Driver'].nunique()}")
    print(f"\nFirst few records:")
    print(df.head(10))
    print(f"\nDriver lap counts:")
    print(df['Driver'].value_counts().sort_index())


if __name__ == "__main__":
    create_f1_sample_data()
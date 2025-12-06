# F1 Lap Times Analysis Pipeline

A production-ready data engineering solution for analyzing Formula 1 driver lap times. This pipeline processes CSV data to calculate average lap times and identify the top performing drivers.

##  Overview

This project demonstrates best practices in data engineering including:
- Clean, modular, object-oriented Python code
- Comprehensive error handling and validation
- Extensive unit testing
- Clear documentation and logging
- Multiple output formats (CSV, JSON)
- Type hints for better code maintainability

##  Problem Statement

Build a batch processing pipeline that:
1. Ingests F1 driver lap times from a CSV file
2. Calculates the average lap time for each driver
3. Identifies and outputs the top 3 fastest drivers
4. Includes each driver's fastest and average lap times

## Architecture

```
Input CSV → Extract → Transform → Load → Output (CSV/JSON)
              ↓         ↓          ↓
           Validate  Calculate  Format
                     Average
```

### Components

- **`f1_lap_times_pipeline.py`**: Main ETL pipeline implementation
- **`generate_sample_data.py`**: Sample data generator for testing
- **`test_f1_pipeline.py`**: Comprehensive unit tests



### Usage

#### 1. Generate Sample Data (Optional)

```bash
python generate_sample_data.py
```

This creates `f1_lap_times.csv` with realistic F1 lap times for 15 drivers.

#### 2. Run the Pipeline

```bash
python f1_lap_times_pipeline.py
```

The pipeline will:
- Read `f1_lap_times.csv`
- Process the data
- Generate output files:
  - `top_3_drivers.csv` - Top 3 drivers in CSV format
  - `top_3_drivers.json` - Top 3 drivers in JSON format
- Display a formatted summary in the console

#### 3. Run Tests

```bash
python test_f1_pipeline.py
```

##  Input Data Format

The pipeline expects a CSV file with the following structure:

```csv
Driver,Time
Hamilton,4.32
Verstappen,4.28
Hamilton,4.45
...
```

**Requirements:**
- **Driver**: String - Driver name
- **Time**: Float - Lap time in seconds (must be positive)
- Minimum 10 unique drivers
- Minimum 3 lap times per driver

## Output Format

### CSV Output (`top_3_drivers.csv`)

```csv
Driver,average_time,fastest_time,lap_count,rank
Verstappen,4.293,4.280,3,1
Hamilton,4.383,4.320,3,2
Leclerc,4.490,4.480,2,3
```

### JSON Output (`top_3_drivers.json`)

```json
{
  "top_drivers": [
    {
      "Driver": "Verstappen",
      "average_time": 4.293,
      "fastest_time": 4.280,
      "lap_count": 3,
      "rank": 1
    },
    ...
  ],
  "total_drivers_analyzed": 15,
  "total_laps_analyzed": 75
}
```

## Pipeline Features

### Data Validation

The pipeline includes robust validation:
- ✅ Checks for required columns
- ✅ Validates data types
- ✅ Ensures no negative lap times
- ✅ Handles empty files gracefully
- ✅ Provides clear error messages

### Error Handling

- Graceful handling of missing files
- Detailed logging for debugging
- Comprehensive exception handling
- Validation at each pipeline stage

### Code Quality

- **Type hints** for better IDE support and documentation
- **Docstrings** for all classes and methods
- **Logging** for tracking pipeline execution
- **Modular design** for easy maintenance and testing
- **PEP 8** compliant code style

##  Testing

The project includes comprehensive unit tests covering:
- Successful data extraction and transformation
- Error handling (missing files, invalid data)
- Edge cases (single driver, identical times)
- Output validation (CSV and JSON)
- Data validation logic

**Test Coverage:**
- 15+ test cases
- All critical code paths covered
- Edge cases and error scenarios

Run tests with:
```bash
python test_f1_pipeline.py
```

##  API Documentation

### F1LapTimesProcessor Class

Main class for processing F1 lap times data.

#### Methods

**`__init__(input_file: str)`**
- Initialize processor with input file path

**`extract() -> pd.DataFrame`**
- Read and validate data from CSV file
- Returns: DataFrame with lap times data

**`transform() -> pd.DataFrame`**
- Calculate averages and rankings
- Returns: DataFrame with processed results

**`get_top_n(n: int = 3) -> pd.DataFrame`**
- Get top N fastest drivers
- Args: n - number of drivers to return
- Returns: DataFrame with top N drivers

**`load_csv(output_file: str, top_n: int = 3) -> None`**
- Save results to CSV format

**`load_json(output_file: str, top_n: int = 3) -> None`**
- Save results to JSON format

**`run_pipeline(...) -> None`**
- Execute complete ETL pipeline

## Design Decisions

### Why Pandas?
- Industry-standard for data manipulation
- Efficient groupby operations
- Easy CSV I/O
- Clean, readable code

### Why Class-Based Design?
- Encapsulation of pipeline logic
- Easy to test individual methods
- Reusable and extensible
- Clear separation of concerns

### Why Multiple Output Formats?
- CSV for data analysis tools (Excel, Tableau)
- JSON for web applications and APIs
- Demonstrates flexibility and production readiness

## Extending the Pipeline

The pipeline is designed to be easily extended:

```python
# Custom processing
processor = F1LapTimesProcessor('input.csv')
processor.extract()
processor.transform()

# Access intermediate results
all_drivers = processor.results
top_5 = processor.get_top_n(5)

# Custom output
processor.load_csv('custom_output.csv', top_n=10)
```

##  Assumptions

1. **Data Quality**: Input data is assumed to be in correct format (Driver, Time columns)
2. **Lap Times**: Time values are in seconds and positive
3. **Driver Names**: Consistent spelling for same driver across all records
4. **Performance**: Dataset fits in memory (suitable for batch processing)

## Future Enhancements

Potential improvements for production deployment:

- [ ] Add support for streaming data (Apache Kafka, Pub/Sub)
- [ ] Implement data quality monitoring
- [ ] Add configuration file for pipeline parameters
- [ ] Create Docker container for easy deployment
- [ ] Add Apache Airflow DAG for scheduling
- [ ] Implement data lineage tracking
- [ ] Add metrics dashboard (Grafana)
- [ ] Support for partitioned data processing
- [ ] Integration with cloud storage (GCS, S3)
- [ ] Add data versioning with DVC

##  License

This project is created for the Peacock Data Engineering assessment.

##  Author

Data Engineering Candidate - Subirna
Assessment Date: December 2025

##  Acknowledgments

- Formula 1 for inspiring the use case
- Peacock/NBCUniversal for the opportunity
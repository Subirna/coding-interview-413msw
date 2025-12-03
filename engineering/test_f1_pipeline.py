# Unit Tests for F1 Lap Times Pipeline

import unittest
import pandas as pd
import os
import json
from f1_lap_times_pipeline import F1LapTimesProcessor


class TestF1LapTimesProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_input_file = 'test_lap_times.csv'
        cls.test_output_csv = 'test_output.csv'
        cls.test_output_json = 'test_output.json'
        
        # Create sample data
        test_data = {
            'Driver': ['Hamilton', 'Verstappen', 'Hamilton', 'Verstappen', 
                      'Leclerc', 'Hamilton', 'Leclerc', 'Verstappen'],
            'Time': [4.32, 4.28, 4.45, 4.31, 4.50, 4.38, 4.48, 4.29]
        }
        df = pd.DataFrame(test_data)
        df.to_csv(cls.test_input_file, index=False)
    
    @classmethod
    def tearDownClass(cls):
        test_files = [
            cls.test_input_file,
            cls.test_output_csv,
            cls.test_output_json
        ]
        for file in test_files:
            if os.path.exists(file):
                os.remove(file)
    
    def setUp(self):
        # Initialize processor for each test
        self.processor = F1LapTimesProcessor(self.test_input_file)
    
    def test_extract_success(self):
        # Test successful data extraction
        df = self.processor.extract()
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 8)
        self.assertTrue('Driver' in df.columns)
        self.assertTrue('Time' in df.columns)
    
    def test_extract_file_not_found(self):
        #  Test extraction with non-existent file
        processor = F1LapTimesProcessor('nonexistent.csv')
        with self.assertRaises(FileNotFoundError):
            processor.extract()
    
    def test_validate_data_success(self):
        # Test data validation with valid data
        df = pd.DataFrame({
            'Driver': ['Hamilton', 'Verstappen'],
            'Time': [4.32, 4.28]
        })
        self.assertTrue(self.processor.validate_data(df))
    
    def test_validate_data_missing_columns(self):
        # Test data validation with missing columns
        df = pd.DataFrame({
            'Driver': ['Hamilton'],
            'WrongColumn': [4.32]
        })
        with self.assertRaises(ValueError):
            self.processor.validate_data(df)
    
    def test_validate_data_negative_times(self):
        # Test data validation with negative times
        df = pd.DataFrame({
            'Driver': ['Hamilton'],
            'Time': [-4.32]
        })
        with self.assertRaises(ValueError):
            self.processor.validate_data(df)
    
    def test_validate_data_empty(self):
        # Test data validation with empty dataframe
        df = pd.DataFrame(columns=['Driver', 'Time'])
        with self.assertRaises(ValueError):
            self.processor.validate_data(df)
    
    def test_transform_calculates_averages(self):
        # Test that transform correctly calculates averages
        self.processor.extract()
        results = self.processor.transform()
        
        # Check that results exist
        self.assertIsNotNone(results)
        self.assertEqual(len(results), 3)  # 3 unique drivers
        
        # Verify columns
        expected_columns = ['Driver', 'average_time', 'fastest_time', 'lap_count', 'rank']
        for col in expected_columns:
            self.assertTrue(col in results.columns)
        
        # Check that results are sorted by average time
        avg_times = results['average_time'].tolist()
        self.assertEqual(avg_times, sorted(avg_times))
    
    def test_transform_without_extract(self):
        """Test transform called without extract."""
        with self.assertRaises(ValueError):
            self.processor.transform()
    
    def test_get_top_n(self):
        """Test getting top N drivers."""
        self.processor.extract()
        self.processor.transform()
        
        top_2 = self.processor.get_top_n(2)
        self.assertEqual(len(top_2), 2)
        
        top_5 = self.processor.get_top_n(5)
        # Should return only 3 since we have 3 drivers
        self.assertEqual(len(top_5), 3)
    
    def test_get_top_n_without_results(self):
        """Test get_top_n called without results."""
        with self.assertRaises(ValueError):
            self.processor.get_top_n(3)
    
    def test_load_csv(self):
        """Test CSV output."""
        self.processor.extract()
        self.processor.transform()
        self.processor.load_csv(self.test_output_csv, top_n=2)
        
        # Verify file exists
        self.assertTrue(os.path.exists(self.test_output_csv))
        
        # Verify content
        output_df = pd.read_csv(self.test_output_csv)
        self.assertEqual(len(output_df), 2)
        self.assertTrue('Driver' in output_df.columns)
        self.assertTrue('average_time' in output_df.columns)
    
    def test_load_json(self):
        """Test JSON output."""
        self.processor.extract()
        self.processor.transform()
        self.processor.load_json(self.test_output_json, top_n=2)
        
        # Verify file exists
        self.assertTrue(os.path.exists(self.test_output_json))
        
        # Verify content
        with open(self.test_output_json, 'r') as f:
            output_data = json.load(f)
        
        self.assertTrue('top_drivers' in output_data)
        self.assertTrue('total_drivers_analyzed' in output_data)
        self.assertTrue('total_laps_analyzed' in output_data)
        self.assertEqual(len(output_data['top_drivers']), 2)
    
    def test_ranking_order(self):
        #  Test that drivers are ranked correctly (fastest = rank 1)
        self.processor.extract()
        results = self.processor.transform()
        
        # Verify rank 1 has the lowest average time
        rank_1_driver = results[results['rank'] == 1].iloc[0]
        self.assertEqual(rank_1_driver['average_time'], 
                        results['average_time'].min())
    
    def test_fastest_time_per_driver(self):
        #  Test that fastest time is correctly calculated per driver
        self.processor.extract()
        results = self.processor.transform()
        
        # Manually verify one driver
        verstappen_data = self.processor.data[
            self.processor.data['Driver'] == 'Verstappen'
        ]
        expected_fastest = verstappen_data['Time'].min()
        
        actual_fastest = results[
            results['Driver'] == 'Verstappen'
        ]['fastest_time'].iloc[0]
        
        self.assertAlmostEqual(actual_fastest, expected_fastest, places=2)


class TestEdgeCases(unittest.TestCase):
    #  Test edge cases and error handling
    
    def test_single_driver_single_lap(self):
        #  Test with minimal data: one driver, one lap
        test_file = 'edge_case.csv'
        df = pd.DataFrame({
            'Driver': ['Hamilton'],
            'Time': [4.32]
        })
        df.to_csv(test_file, index=False)
        
        try:
            processor = F1LapTimesProcessor(test_file)
            processor.extract()
            results = processor.transform()
            
            self.assertEqual(len(results), 1)
            self.assertEqual(results.iloc[0]['average_time'], 4.32)
            self.assertEqual(results.iloc[0]['fastest_time'], 4.32)
        finally:
            if os.path.exists(test_file):
                os.remove(test_file)
    
    def test_identical_times(self):
        # Test with all identical lap times
        test_file = 'identical.csv'
        df = pd.DataFrame({
            'Driver': ['Hamilton'] * 5,
            'Time': [4.32] * 5
        })
        df.to_csv(test_file, index=False)
        
        try:
            processor = F1LapTimesProcessor(test_file)
            processor.extract()
            results = processor.transform()
            
            self.assertEqual(results.iloc[0]['average_time'], 4.32)
            self.assertEqual(results.iloc[0]['fastest_time'], 4.32)
        finally:
            if os.path.exists(test_file):
                os.remove(test_file)


def run_tests():
    #  Run all tests
    unittest.main(argv=[''], verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()
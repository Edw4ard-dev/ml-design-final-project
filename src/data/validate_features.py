import great_expectations as gx
import os
import numpy as np
import pandas as pd
import unicodedata
import warnings

#Load cleaned data
cleaned_data_path = 'data/processed/spotify_user_behavior_features.parquet'
data = pd.read_parquet(cleaned_data_path)

# Create Data Context.
context = gx.get_context()

# Create Data Source, Data Asset, Batch Definition, and Batch.
data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="Spotify user behavior featured data")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe": data})

# Create an Expectation Suite
suite = gx.ExpectationSuite(name="Spotify user behavior feature expectations")

# Add the Expectation Suite to the Data Context
suite = context.suites.add(suite)

# Validate new columns exist
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='new_user'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='heavy_listener'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='many_playlists'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='dislikes_suggestions'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='likes_personalization'))

# Validate data types of new columns
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='new_user', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='heavy_listener', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='many_playlists', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='dislikes_suggestions', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='likes_personalization', type_="int64"
    ))
    

# Validate values are 0 or 1 for new binary features
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='new_user', value_set=[0, 1]
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='heavy_listener', value_set=[0, 1]
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='many_playlists', value_set=[0, 1]
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='dislikes_suggestions', value_set=[0, 1]
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column='likes_personalization', value_set=[0, 1]
    ))

# Validate results and print summary 
validation_results = batch.validate(suite)
print("Validation Success:", validation_results.success)
if not validation_results.success:
    print("Failed Expectations:")
    for result in validation_results.results:
        if not result.success:
            column = result.expectation_config.kwargs.get('column', 'unknown')
            expected = result.expectation_config.kwargs.get('type_', 'unknown')
            observed = result.result.get('observed_value', 'unknown')
            print(f"- Column '{column}': expected {expected}, observed {observed}")
print("Overall Success:", validation_results.success)

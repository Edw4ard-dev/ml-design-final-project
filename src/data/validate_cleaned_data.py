import great_expectations as gx
import os
import numpy as np
import pandas as pd
import unicodedata
import warnings

#Load cleaned data
cleaned_data_path = 'data/processed/spotify_user_behavior_cleaned.parquet'
data = pd.read_parquet(cleaned_data_path)

# Create Data Context.
context = gx.get_context()

# Create Data Source, Data Asset, Batch Definition, and Batch.
data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="Spotify user behavior cleaned data")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe": data})

# Create an Expectation Suite
suite = gx.ExpectationSuite(name="Spotify user behavior cleaned expectations")

# Add the Expectation Suite to the Data Context
suite = context.suites.add(suite)

# Validate columns exist
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='user_id'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='country'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='age'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='signup_date'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='subscription_type'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='churn'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='months_inactive'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='inactive_3_months_flag'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='ad_interaction'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='ad_conversion_to_subscription'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='music_suggestion_rating_1_to_5'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='avg_listening_hours_per_week'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='favorite_genre'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='most_liked_feature'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='desired_future_feature'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='primary_device'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='playlists_created'))
suite.add_expectation(gx.expectations.ExpectColumnToExist(column='avg_skips_per_day'))

# Validate data types
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='user_id', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='country', type_="CategoricalDtypeType"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='age', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='signup_date', type_="datetime64[ns]" 
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='subscription_type', type_="CategoricalDtypeType"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='churn', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='months_inactive', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='inactive_3_months_flag', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='ad_interaction', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='ad_conversion_to_subscription', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='music_suggestion_rating_1_to_5', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='avg_listening_hours_per_week', type_="float64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='favorite_genre', type_="CategoricalDtypeType"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='most_liked_feature', type_="CategoricalDtypeType"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='desired_future_feature', type_="CategoricalDtypeType"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='primary_device', type_="CategoricalDtypeType"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='playlists_created', type_="int64"
    ))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
    column='avg_skips_per_day', type_="int64"     
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

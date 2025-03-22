import pyarrow as pa 
from pyspark.sql import types 

user_dtype_mapping = {'id': 'int32',
    'current_age': 'int32',
    'retirement_age': 'int32',
    'birth_year': 'int32',
    'birth_month': 'int32',
    'gender': 'string',
    'address': 'string',
    'latitude': 'float32',
    'longitude': 'float32',
    'per_capita_income': 'float32',
    'yearly_income': 'float32',
    'total_debt': 'float32',
    'credit_score': 'int32',
    'num_credit_cards': 'int32'}


cards_dtype_mapping = {
    'id': 'int32',
    'client_id': 'int32',
    'card_brand': 'string',
    'card_type': 'string',
    'card_number': 'string',
    'expires': 'datetime64[ns]',
    'cvv': 'int32',
    'has_chip': 'string',
    'num_cards_issued': 'int32',
    'credit_limit': 'float32',
    'acct_open_date': 'datetime64[ns]',
    'year_pin_last_changed': 'int32',
    'card_on_dark_web': 'string'
}

trnsction_schema = types.StructType([
            types.StructField('id', types.IntegerType(), True), 
            types.StructField('date', types.StringType(), True), 
            types.StructField('client_id', types.IntegerType(), True), 
            types.StructField('card_id', types.IntegerType(), True), 
            types.StructField('amount', types.StringType(), True), 
            types.StructField('use_chip', types.StringType(), True), 
            types.StructField('merchant_id', types.IntegerType(), True), 
            types.StructField('merchant_city', types.StringType(), True), 
            types.StructField('merchant_state', types.StringType(), True), 
            types.StructField('zip', types.StringType(), True), 
            types.StructField('mcc', types.StringType(), True), 
            types.StructField('errors', types.StringType(), True)])
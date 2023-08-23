import re

def transform_data(data):

    """
    Apply data cleaning, data quality and data enrichment rules.
        
    """

    SINK_COLUMNS =['Sociedad',
                   'CtaMayor',
                   'ImporteML',
                   'Moneda',                   
                   'ImporteMD',
                   'MonDoc']
    
    AGG_COLUMNS = [ 'Sociedad',
                    'CtaMayor',
                    'Moneda',
                    'MonDoc']
    
    # Get only sink columns from data
    data_transformed =  data[SINK_COLUMNS]

    data_transformed['ImporteML'] = data_transformed['ImporteML'].apply(lambda x: format_negatives(x)).astype(float)
    data_transformed['ImporteMD'] = data_transformed['ImporteMD'].apply(lambda x: format_negatives(x)).astype(float)

    # data_transformed.loc[:,'ImporteML'] = data_transformed.loc[:,'ImporteML'].apply(lambda x: format_negatives(x)).astype(float)

    # Summarize imports
    aggregate_data = data_transformed.groupby(AGG_COLUMNS).agg({'ImporteML':'sum','ImporteMD':'sum'}).reset_index()

    return aggregate_data

def format_negatives(str_value):
    value = str_value.replace('.','')
    value = value.replace(',','.')

    if re.search('-',value):
        value = value.replace('-','')
        value = '-' + value    
    
    return value
    


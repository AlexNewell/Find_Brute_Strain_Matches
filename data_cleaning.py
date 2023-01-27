import pandas as pd
import numpy as np
from scipy import stats


# create a function to summarize the data quality for categorical variables
# create a function to summarize the data quality
def dataquality_numeric(raw):
    numeric_col = raw.select_dtypes(include = ['int64', 'float64'])
    summary = pd.DataFrame(numeric_col.dtypes, columns = ['dtypes'])
    summary = summary.reset_index()
    summary['Name'] = summary['index']
    summary = summary[['Name', 'dtypes']]
    summary['# of Records'] = numeric_col.count().values
    summary['% populated'] = (((len(raw) - numeric_col.isnull().sum().values) / len(raw)) * 100).round(2)
    summary['# Zeros'] = numeric_col[numeric_col == 0].count().values
    summary['Uniques'] = numeric_col.nunique().values
    summary['Mean'] = numeric_col.mean().values
    summary['Maximum'] = numeric_col.max().values
    summary['Minimum'] = numeric_col.min().values
    summary['Standard Deviation'] = numeric_col.std().values

    # Calculate the entropy of a distribution for given probability values.
    for name in summary['Name'].value_counts().index:
        summary.loc[summary['Name'] == name, 'Entropy'] = round(
            stats.entropy(numeric_col[name].value_counts(normalize = True), base = 2), 2)
    return summary


# create a function to summarize the data quality for categorical variables
def dataquality_categorical(raw):
    categorical_col = raw.select_dtypes(include = ['object', 'category'])
    summary = pd.DataFrame(categorical_col.dtypes, columns = ['dtypes'])
    summary = summary.reset_index()
    summary['Name'] = summary['index']
    summary = summary[['Name', 'dtypes']]
    summary['# of Records'] = categorical_col.count().values
    summary['% populated'] = (((len(raw) - categorical_col.isnull().sum().values) / (len(raw))) * 100).round(2)
    summary['# NA'] = categorical_col.isnull().sum().values \
                      + categorical_col[categorical_col == '-1'].count().values
    summary['# Unique_Values'] = categorical_col.nunique().values
    summary['First_Value'] = categorical_col.loc[0].values
    summary['Second_Value'] = categorical_col.loc[1].values
    summary['Third_Value'] = categorical_col.loc[2].values
    count_value = pd.DataFrame(index = categorical_col.columns, columns = ['count'])
    most_common = pd.DataFrame(index = categorical_col.columns, columns = ['MostCommon'])

    for col in categorical_col.columns:
        count_value.loc[col, 'count'] = categorical_col[col].value_counts(normalize = True).values[0] * 100
        most_common.loc[col, 'MostCommon'] = categorical_col[col].mode().values
        summary['% Most_Common_Field'] = count_value['count'].values.astype(float).round(2)
        summary['Most_Common_Field'] = most_common['MostCommon'].values

    # Calculate the entropy of a distribution for given probability values.
    for name in summary['Name'].value_counts().index:
        summary.loc[summary['Name'] == name, 'Entropy'] = round(
            stats.entropy(categorical_col[name].value_counts(normalize = True), base = 2), 2)
    return summary


def standardize_cann(input_df):
    # Cannabinoids with an 'a' variant
    cannab_a_list = ['CBD', 'CBG', 'CBN'
                    , 'CBC', 'CBDV', 'THCV'
                    , 'CBL']
    # Add 'a' versions to main version for each cannabinoid
    for cannab_a in cannab_a_list:
        input_df[cannab_a] = input_df[cannab_a] + input_df[cannab_a + 'a']

    # Separate: THC naming doesn't follow same cann / cann-a rule
    input_df['THC'] = input_df['THCa'] + input_df['d9THC']

    # Create percentages for cannabinoids
    # Divide each cannabinoid by 1000 for parts per thousand
    cannab_per_k = ['THC', 'CBD', 'CBG'
                    , 'CBN', 'CBC', 'CBDV'
                    , 'THCV', 'CBL', 'd8THC', 'Total_Cannabinoids']
    for col in cannab_per_k:
        input_df[col] = input_df[col] / 1000

    # Remaining standardizations
    input_df = input_df.rename({'d8THC': 'D8'}, axis=1)

    # Then Calculate percent of each terpene relative to total terpenes
    cannabis_vol_column = ['THC', 'CBD', 'CBG', 'CBN'
                            , 'CBC', 'CBDV', 'THCV', 'CBL', 'D8']
    for col in cannabis_vol_column:
        input_df[f'{col}_vs_Cann'] = input_df[col] / input_df['Total_Cannabinoids']

    thc_divide_list_cann = ['CBD']
    for divider in thc_divide_list_cann:
        input_df['THC_vs_' + divider] = input_df['THC'] / input_df[divider + '_vs_Cann']

    return input_df



def standardize_terp(input_df):
    terpenes_main = ['A_Bisabolol', 'B_Myrcene', 'B_Caryophyllene'
                    , 'D_Limonene', 'A_Pinene', 'B_Pinene'
                    , 'A_Humulene', 'Terpinolene', 'Linalool'
                    , 'Ocimene', 'Nerolidol', 'Total_Terpenes']

    # Other terpene standardizations
    input_df['Nerolidol'] = input_df['cis_Nerolidol'] + input_df['trans_Nerolidol']

    # Change terpenes from mg to % in float format
    for float_col in terpenes_main:
        input_df[float_col] = input_df[float_col] / 1000

    # Create a column that has the total value of non-main terpenes
    input_df['Other_Terpenes'] = input_df['Total_Terpenes'] - (input_df[terpenes_main].sum(axis = 1))
    input_df['Other_Terpenes'] = input_df['Other_Terpenes'].apply(lambda x: 0 if x < 0 else x)

    # Copy full list of Terpenes and add Other to list
    # Then Calculate percent of each terpene relative to total terpenes
    terpenes_col = terpenes_main.copy()
    terpenes_col.extend(['Other_Terpenes'])
    for col in terpenes_col:
        input_df[f'{col}_vs_Terpenes'] = input_df[col] / input_df['Total_Terpenes']

    return input_df



def clean_product_data(engine):
    # Load product data
    Product = pd.read_sql_table('Product', engine, columns = ['product_uid', 'strain', 'matrix'])
    Product = Product.drop_duplicates()
    Product = Product.set_index('product_uid')
    Product['matrix'] = Product['matrix'].replace('Concentrate', 'Concentrates & Extracts')
    Product['matrix'] = Product['matrix'].replace('Plant - Cured', 'Plant')
    Product['strain'] = Product['strain'].str.lower()
    Product['strain'] = Product['strain'].str.replace('[^a-zA-Z0-9]', '', regex = True).str.strip()

    # Load product data: cannabinoids
    product_cann = pd.read_sql_table('Product_Cannabinoids', engine)
    product_cann = product_cann.set_index('product_uid')
    product_cann = product_cann.drop('index', axis = 1)
    product_cann = product_cann.apply(pd.to_numeric, errors = 'coerce')
    product_cann = standardize_cann(product_cann)

    # Load product data: terpenes
    product_terp = pd.read_sql_table('Product_Terpenes', engine)
    product_terp = product_terp.set_index('product_uid')
    product_terp = product_terp.drop('index', axis = 1)
    product_terp = product_terp.rename({'Nerol': 'Nerolidol'
                                        , 'A-Bisabalol':'A_Bisabolol'}, axis = 1)
    product_terp = product_terp.apply(pd.to_numeric, errors = 'coerce')
    product_terp = standardize_terp(product_terp)

    # In case of multiple records per product_uid, take the mean values
    product_cann_mean = product_cann.groupby(['product_uid'])[product_cann.columns].mean()
    product_terp_mean = product_terp.groupby(['product_uid'])[product_terp.columns].mean()

    # Input file for cleaning process
    product_pre = Product.merge(product_cann_mean, on = 'product_uid', how = 'inner')
    product_pre = product_pre.merge(product_terp_mean, on = 'product_uid', how = 'inner')

    # Drop if no terpenes or is edibles
    PRE_drop_records = product_pre[(product_pre['Total_Terpenes'].isna())
                                   | (product_pre['Total_Terpenes'] == 0)
                                   | (product_pre['matrix'] == 'Edible')
                                   ].index
    product_pre = product_pre.drop(PRE_drop_records)
    product_pre = product_pre.drop('matrix', axis = 1)

    product_pre['Total_Weight'] = product_pre['Total_Terpenes'] + product_pre['Total_Cannabinoids']
    product_pre['Terpenes_vs_Total'] = product_pre['Total_Terpenes'] / product_pre['Total_Weight']
    product_pre['Cannabinoids_vs_Total'] = product_pre['Total_Cannabinoids'] / product_pre['Total_Weight']

    thc_divide_list_terp = ['B_Caryophyllene', 'A_Pinene', 'B_Myrcene'
                            , 'D_Limonene', 'Linalool', 'Nerolidol']
    for divider in thc_divide_list_terp:
        product_pre['THC_vs_' + divider] = product_pre['THC'] / product_pre[divider + '_vs_Terpenes']
    # Separate due to naming
    product_pre['THC_vs_' + 'Terpenes'] = product_pre['THC'] / product_pre['Total_Terpenes']

    product_pre = product_pre.fillna(0)

    # Find what columns have np.inf issue
    numeric_DQR_raw = dataquality_numeric(product_pre)
    col_temp = numeric_DQR_raw[numeric_DQR_raw['Mean'] == np.inf]['Name'].values

    for col in col_temp:
        themax = product_pre[col].replace([np.inf, -np.inf], np.nan).dropna().max()
        product_pre[col].replace([np.inf, -np.inf], themax, inplace = True)

    product_pre = product_pre.drop_duplicates()
    product_pre = product_pre.reset_index(drop = False)
    # Just in case, drop any duplicates that were created
    product_pre = product_pre.drop_duplicates(subset = 'product_uid')

    return(product_pre)

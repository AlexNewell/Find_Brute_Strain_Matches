# reate synthetic strain-level COAs
# - Direct strain level
# - Master strain level

import pandas as pd

global flower_codes
flower_codes = ['f', 'p']

global concentrate_codes
concentrate_codes = ['c', 'v']


def create_median_coas(input_df, product_types, field_pivot, min_n = 1):
	products_df = input_df[input_df['Product_Category'].isin(product_types)].copy()

	strain_counts = products_df.groupby([field_pivot])[['product_uid']].nunique()
	strain_counts = strain_counts.rename({'product_uid': 'count'}, axis = 1)
	strains_yes = strain_counts[strain_counts['count'] > min_n].index.values

	products_df = products_df[products_df[field_pivot].isin(strains_yes)]

	products_df = products_df.drop(['product_uid', 'Product_Category'], axis = 1)

	products_df = products_df.set_index(field_pivot)

	products_df_median = products_df.groupby([field_pivot]).median()
	products_df_median = products_df_median.reset_index(drop = False)

	return products_df_median


def copy_median_coas_by_product_type(input_df, product_type_codes):
	output_df = pd.DataFrame()
	for code in product_type_codes:
		product_type_pre_median = input_df.copy()
		product_type_pre_median['Product_Category'] = code
		output_df = pd.concat([output_df, product_type_pre_median])
	return output_df


def create_combine_median_coas(input_coas, target_pivot):
	median_coas_pivot = pd.DataFrame()

	product_type_split = {'concentrate_codes': concentrate_codes
							, 'flower_codes': flower_codes}

	# Generate medians by flower vs concentrate product types
	for type_split_codes in product_type_split.values():
		split_codes_coas_median = create_median_coas(input_coas
													 , type_split_codes
													 , target_pivot)

		# Create a copy for each split type, i.e. one for "f" and one for "p" product types
		split_codes_coas_median = copy_median_coas_by_product_type(split_codes_coas_median
																   , type_split_codes)

		median_coas_pivot = pd.concat([median_coas_pivot, split_codes_coas_median])

	return median_coas_pivot


def find_brute_match(input_df, master_strain_list):
	# # Keep the strain names that need to be compared to the mastered strain name list
	input_strain = input_df[['strain']].drop_duplicates()

	# Do an all-to-all join, then reduce down to just the records where the strain name contains a master strain name
	input_strain['join'] = 1
	master_strain_list['join'] = 1
	brute_result = input_strain.merge(master_strain_list, on = 'join').drop('join', axis = 1)

	# Keep records where the master_strain can be found within the strain name
	brute_result = brute_result[brute_result.apply(lambda x: x.strain.find(x.master_strain), axis = 1).ge(0)]

	brute_result = brute_result.sort_values(by = ['strain', 'master_strain']).reset_index(drop = True)

	return brute_result


def create_strain_brute_median_coas(product_coas, master_strain_list):
	product_coas['Product_Category'] = product_coas['product_uid'].str.strip().str[-1]

	# Reduce to just flower and concentrate products. Excludes edibles / other
	product_coas = product_coas[(product_coas['Product_Category'].isin(flower_codes))
								| (product_coas['Product_Category'].isin(concentrate_codes))].drop_duplicates()

	full_strain_median = create_combine_median_coas(product_coas, 'strain')

	# NOTE: brute force merging takes a long time due to the all-to-all merge. Use this sparingly

	product_brute = find_brute_match(input_df = product_coas
									 , master_strain_list = master_strain_list)

	# Add chemical profile back on to mastered strain
	product_brute = product_brute.merge(product_coas
												, on = ['strain']
												, how = 'left')

	product_brute = product_brute.drop_duplicates()

	full_brute_median = create_combine_median_coas(product_brute, 'master_strain')

	return full_strain_median, full_brute_median

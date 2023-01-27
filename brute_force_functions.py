import sqlalchemy as sa
import pandas as pd


def create_sa_metadata_object(target_table, engine):
	# Create the Metadata Object
	meta_data = sa.MetaData(bind = engine)
	sa.MetaData.reflect(meta_data)

	# Get the table from the Metadata object
	table = meta_data.tables[target_table]

	return table


def run_sa_query(select_col, engine):
	query = sa.select(sa.distinct(select_col))
	result = pd.read_sql(query, con = engine)
	return result


def get_strains(target_table, engine):
	# Create SQLAlchemy metadata table object
	table = create_sa_metadata_object(target_table, engine)

	# Define query parameters
	select_col = table.c.strain

	# Fetch all the records
	result = run_sa_query(select_col, engine)

	return result


def load_strains(db_table, engine):
	strains_list = get_strains(db_table, engine)
	strains_list = strains_list.drop_duplicates()
	return strains_list


def append_strains(strains_holder, db_table, engine):
	strains_list = load_strains(db_table, engine)
	print(db_table, 'strains found', strains_list.shape[0])
	strains_holder = pd.concat([strains_holder, strains_list], axis = 0)
	strains_holder = strains_holder.drop_duplicates()
	return strains_holder


def format_strains(strains_holder):
	strains_holder['strain'] = strains_holder['strain'].str.lower()
	strains_holder['strain'] = strains_holder['strain'].str.replace('[^a-zA-Z0-9]', '', regex = True).str.strip()
	strains_holder = strains_holder.drop_duplicates()
	strains_holder = strains_holder[strains_holder['strain'] != '']
	strains_holder = strains_holder.sort_values(by = 'strain')
	strains_holder = strains_holder.reset_index(drop = True)
	return strains_holder


def find_brute_match(input_df, master_list):
	# # Keep the strain names that need to be compared to the mastered strain name list
	input_strain = input_df[['strain']].drop_duplicates()
	master = master_list.copy()

	# Do an all-to-all join, then reduce down to just the records where the strain name contains a master strain name
	input_strain['join'] = 1
	master['join'] = 1

	brute_result = input_strain.merge(master, on = 'join').drop('join', axis = 1)
	# Keep records where the master_strain can be found within the strain name
	brute_result = brute_result[brute_result.apply(lambda x: x.strain.find(x.master_strain), axis = 1).ge(0)]
	brute_result = brute_result.sort_values(by = ['strain', 'master_strain']).reset_index(drop = True)

	return brute_result

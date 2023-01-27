from brute_force_functions import *
from qa_functions import qa_codetimer_start, qa_codetimer_end
from create_AWS_Postgres_engine import create_aws_postgres_engine
import pandas as pd

# --------------------------------------------------------------------------------------------------------
# SECTION: Configuration
# --------------------------------------------------------------------------------------------------------
qa_run = True
env = 'prod'
Main_Folder = r'/home/ec2-user'

engine = create_aws_postgres_engine(env = 'prod'
                                    , config_loc = Main_Folder)

# --------------------------------------------------------------------------------------------------------
# SECTION: Load all possible strains found in Retail and Product tables for brute force matching
# --------------------------------------------------------------------------------------------------------


# Load retail strains
qa_codetimer_start(qa_run)

strains_holder = pd.DataFrame()
strains_holder = append_strains(strains_holder, 'Retail', engine)
strains_holder = append_strains(strains_holder, 'Retail_Historical', engine)
strains_holder = append_strains(strains_holder, 'Product', engine)

strains_holder = format_strains(strains_holder)

print('Count of strains_holder', strains_holder.shape[0])

qa_codetimer_end(qa_run, 'Load data')

# --------------------------------------------------------------------------------------------------------
# SECTION: Remove any strains from list that have already been matched (found on existing table)
# --------------------------------------------------------------------------------------------------------
qa_codetimer_start(qa_run)

existing_strain_brute_matches = pd.read_sql_table('Strain_Brute_Matches', con = engine, columns = ['strain'])
existing_strain_brute_matches = existing_strain_brute_matches.drop_duplicates()
print('Count of existing_strain_brute_matches', existing_strain_brute_matches.shape[0])

strains_holder = strains_holder[~strains_holder['strain'].isin(existing_strain_brute_matches['strain'].values)]

print('Count of strains_holder (remainder)', strains_holder.shape[0])

qa_codetimer_end(qa_run, 'Remove existing matches')

# If there are records to process:
if strains_holder.shape[0] > 0:

    # --------------------------------------------------------------------------------------------------------
    # SECTION: For remaining unmatched strains, find brute force match and append to existing matches
    # --------------------------------------------------------------------------------------------------------
    qa_codetimer_start(qa_run)

    master_strain_list = pd.read_sql_table('Master_Strain_List'
                                           , columns = ['strain']
                                           , con = engine)
    # Rename to avoid confusion with the 'strain' field on other dfs
    master_strain_list = master_strain_list.rename({'strain': 'master_strain'}
                                                   , axis = 1)

    strain_brute_matches = find_brute_match(input_df = strains_holder
                                            , master_list = master_strain_list)

    # Isolate strains that were not successfully matched
    # and prep them to be appended with a blank master_strain value to successes
    strains_holder_y = strain_brute_matches['strain'].drop_duplicates().values
    strains_holder_n = strains_holder[~strains_holder['strain'].isin(strains_holder_y)].copy()
    strains_holder_n['master_strain'] = ''

    strain_brute_out = pd.concat([strain_brute_matches, strains_holder_n], axis = 0)

    # NOTE: will only return matches; any unmatched strains will not be on this output list

    qa_codetimer_end(qa_run, 'find_brute_match')

    # --------------------------------------------------------------------------------------------------------
    # SECTION: Write list out to Postgres for use by other processes (at least CRE; PRE)
    # --------------------------------------------------------------------------------------------------------
    qa_codetimer_start(qa_run)
    strain_brute_out = pd.concat([existing_strain_brute_matches
                                         , strain_brute_out]
                                 , axis = 0)

    print('Count of new strain_brute_out', strain_brute_out.shape[0])

    strain_brute_out.to_sql('Strain_Brute_Matches'
                            , engine
                            , index = False
                            , if_exists = 'replace')

    qa_codetimer_end(qa_run, 'Write brute force matches back to Postgres')
import pandas as pd
import numpy as np
from itertools import product
import time

df = pd.read_csv('ERP_data_test.csv', dtype = {
    'PART_NUM' : 'string',
    'ABLE_CODE' : 'string',
    'UNSPSC_CODE' : 'string',
    'NAICS_CODE' : 'string',
})
df.columns = df.columns.str.strip()
# unique_df = df.drop_duplicates(subset='PART_NUM').reset_index(drop=True)



def update_top_5(existing_df, new_df):
    # If existing_df is empty, just return new_df
    if existing_df is None or existing_df.empty:
        return new_df

    # Concatenate existing and new results
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    # Sort and keep the top 5 matches for each source part number
    top_5_df = combined_df.sort_values(
        ['source_part_number', 'match_score'], ascending=[True, False]
    ).groupby('source_part_number').head(5)

    return top_5_df


def compute_match_score(df, chunk_size_source=10, chunk_size_target=100):
    # Initialize an empty DataFrame to store the top 5 results
    top_5_results = pd.DataFrame(columns=['source_part_number', 'target_part_number', 'match_score'])

    # Create a unique list of part numbers
    part_numbers = df['PART_NUM'].unique()

    # Process each source chunk
    for i in range(0, len(part_numbers), chunk_size_source):
        source_chunk = part_numbers[i:i + chunk_size_source]

        # Initialize a temporary DataFrame to store the best matches for this source chunk
        temp_results = pd.DataFrame(columns=['source_part_number', 'target_part_number', 'match_score'])

        # Process each target chunk
        for j in range(0, len(part_numbers), chunk_size_target):
            target_chunk = part_numbers[j:j + chunk_size_target]

            # Merge the source and target chunks for comparison
            merged_df = pd.merge(
                df[df['PART_NUM'].isin(source_chunk)],
                df[df['PART_NUM'].isin(target_chunk)],
                how='cross',
                suffixes=('_source', '_target')
            )

            # Exclude comparisons where source and target part numbers are the same
            merged_df = merged_df[merged_df['PART_NUM_source'] != merged_df['PART_NUM_target']]

            # Compute the match score, handling NaN values
            merged_df['ABLE_MATCH'] = (
                                              merged_df['ABLE_CODE_source'] == merged_df['ABLE_CODE_target']
                                      ) & merged_df[['ABLE_CODE_source', 'ABLE_CODE_target']].notna().all(axis=1)

            merged_df['UNSPSC_MATCH'] = (
                                                merged_df['UNSPSC_CODE_source'] == merged_df['UNSPSC_CODE_target']
                                        ) & merged_df[['UNSPSC_CODE_source', 'UNSPSC_CODE_target']].notna().all(axis=1)

            merged_df['NAICS_MATCH'] = (
                                               merged_df['NAICS_CODE_source'] == merged_df['NAICS_CODE_target']
                                       ) & merged_df[['NAICS_CODE_source', 'NAICS_CODE_target']].notna().all(axis=1)

            merged_df['match_score'] = (
                    merged_df['ABLE_MATCH'].astype(int) +
                    merged_df['UNSPSC_MATCH'].astype(int) +
                    merged_df['NAICS_MATCH'].astype(int)
            )

            # Remove duplicate target part numbers before selecting top 5
            merged_df = merged_df.drop_duplicates(subset=['PART_NUM_source', 'PART_NUM_target'])

            # Select top 5 matches for this source chunk and target chunk
            chunk_top_5 = merged_df.sort_values(
                ['PART_NUM_source', 'match_score'], ascending=[True, False]
            ).groupby('PART_NUM_source').head(5)

            # Rename columns to match the final output
            chunk_top_5 = chunk_top_5.rename(columns={
                'PART_NUM_source': 'source_part_number',
                'PART_NUM_target': 'target_part_number'
            })

            # Append the results of this chunk to the temporary results DataFrame
            temp_results = pd.concat([temp_results, chunk_top_5])

        # After comparing all target chunks, update the global top 5 results
        top_5_results = update_top_5(top_5_results, temp_results)

    # Remove any duplicate target part numbers for the same source part number
    final_results = top_5_results.sort_values(
        ['source_part_number', 'match_score'], ascending=[True, False]
    ).drop_duplicates(subset=['source_part_number', 'target_part_number'])

    # Ensure that we still only have top 5 results for each source part number
    final_results = final_results.groupby('source_part_number').head(5)

    return final_results

if __name__ == '__main__':
    result = compute_match_score(df)
    result.to_csv('top_matches.csv')
    print(result)

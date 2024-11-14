import pandas as pd

def transform_raw_data(data: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_jobs = normalize_and_rename_job_offers(data)
    df_jobs = extract_location(df_jobs)
    df_jobs = extract_seniority(df_jobs)
    df_jobs = delete_duplicates(df_jobs)
    df_jobs, df_requirements, df_jobs_and_requirements = transform_jobs_and_requirements(df_jobs)
    return df_jobs, df_requirements, df_jobs_and_requirements


def normalize_and_rename_job_offers(data: dict) -> pd.DataFrame:
    df_jobs = pd.json_normalize(
        data["postings"],
        sep="_"
    )
    # Remove columns
    columns_to_keep = [
        'id',
        'name',
        'title',
        'category',
        'seniority',
        'reference',
        'location_places',
        'salary_from',
        'salary_to',
        'salary_type',
        'salary_currency',
        'tiles_values'
    ]

    df_jobs = df_jobs[columns_to_keep]

    # Rename original columns
    df_jobs = df_jobs.rename(columns={
        'id': 'job_id',
        'name': 'company_name',
        'salary_from': 'salary_from',
        'salary_to': 'salary_to',
        'salary_type': 'salary_type',
        'salary_currency': 'salary_currency'
    })

    return df_jobs

def extract_location(df_jobs: pd.DataFrame) -> pd.DataFrame:

    df_jobs['city'] = df_jobs['location_places'].apply(
        lambda x: x[0].get('city') if x and len(x) > 0 else None
    )
    df_jobs = df_jobs.drop('location_places', axis=1)

    return df_jobs

def extract_seniority(df_jobs: pd.DataFrame) -> pd.DataFrame:

    df_jobs['seniority'] = df_jobs['seniority'].apply(
        lambda x: x[0] if isinstance(x, list) and x else None
    )

    return df_jobs

def delete_duplicates(df_jobs: pd.DataFrame) -> pd.DataFrame:
    df_jobs = df_jobs.drop_duplicates(subset=['reference'])

    return df_jobs

def transform_jobs_and_requirements(df_jobs: pd.DataFrame)-> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    sprawdzic czy requiremetns sa w bigquery
    jak sa to zmeinic w liste i zrobic wrzucic
    """
    # First, get existing requirements from BigQuery
    try:
        existing_requirements_query = """
        SELECT requirement_id, requirement_name
        FROM `nfj-elt-project.marts.dim_requirements`
        """
        existing_requirements_df = pd.read_gbq(existing_requirements_query)
        requirements_mapping = dict(zip(
            existing_requirements_df['requirement_name'],
            existing_requirements_df['requirement_id']
        ))
        max_requirement_id = existing_requirements_df['requirement_id'].max()
    except Exception as e:
        requirements_mapping = {}
        max_requirement_id = 0

    requirements_set = set()
    job_requirements_list = []
    new_requirements = []
    requirement_id_counter = max_requirement_id + 1

    # Process jobs and requirements
    for _, row in df_jobs.iterrows():
        job_id = row['job_id']
        if isinstance(row['tiles_values'], list):
            for tile in row['tiles_values']:
                if tile['type'] == 'requirement':
                    requirement_name = tile['value']

                    # Check if requirement exists, if not create new one
                    if requirement_name not in requirements_mapping:
                        requirements_mapping[requirement_name] = requirement_id_counter
                        new_requirements.append({
                            'requirement_id': requirement_id_counter,
                            'requirement_name': requirement_name
                        })
                        requirement_id_counter += 1

                    # Add to job requirements using existing or new ID
                    job_requirements_list.append({
                        'job_id': job_id,
                        'requirement_id': requirements_mapping[requirement_name]
                    })

    # Create dataframes
    df_new_requirements = pd.DataFrame(new_requirements)
    df_job_requirements = pd.DataFrame(job_requirements_list)

    # Drop tiles_values from jobs dataframe
    df_jobs = df_jobs.drop('tiles_values', axis=1)

    current_time = pd.Timestamp.now()
    df_jobs = add_scraped_at_timestamp(df_jobs, current_time)
    df_new_requirements = add_scraped_at_timestamp(df_new_requirements, current_time)
    df_job_requirements = add_scraped_at_timestamp(df_job_requirements, current_time)


    return df_jobs, df_new_requirements, df_job_requirements


def add_scraped_at_timestamp(df: pd.DataFrame, timestamp)-> pd.DataFrame:
    current_time = pd.Timestamp.now()
    df['scraped_at'] = current_time
    df['scraped_at'] = df['scraped_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df


# def transform_jobs_and_requirements(df_jobs: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#     # Create requirements mapping
#     requirements_list = []
#     job_requirements_list = []


#     for _, row in df_jobs.iterrows():
#         job_id = row['job_id']
#         if isinstance(row['tiles_values'], list):
#             for tile in row['tiles_values']:
#                 if tile['type'] == 'requirement':
#                     requirement_name = tile['value']
#                     requirements_list.append(requirement_name)
#                     job_requirements_list.append({
#                         'job_id': job_id,
#                         'requirement_name': requirement_name
#                     })

#     # Create unique requirements with IDs
#     df_requirements = pd.DataFrame({
#         'requirement_id': range(1, len(set(requirements_list)) + 1),
#         'requirement_name': list(set(requirements_list))
#     })

#     # Create job-requirements mapping
#     df_job_requirements = pd.DataFrame(job_requirements_list)

#     # Join with requirements to get requirement_ids
#     df_job_requirements = df_job_requirements.merge(
#         df_requirements,
#         on='requirement_name'
#     )[['job_id', 'requirement_id']]

#     # Drop tiles.values from jobs dataframe as we don't need it anymore
#     df_jobs = df_jobs.drop('tiles_values', axis=1)

#     return df_jobs, df_requirements, pd.DataFrame(df_job_requirements)

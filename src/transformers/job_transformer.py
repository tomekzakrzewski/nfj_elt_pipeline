import pandas as pd

def transform_raw_data(data: dict):
    df_jobs = normalize_and_rename_job_offers(data)
    df_jobs = extract_location(df_jobs)
    df_jobs = extract_seniority(df_jobs)
    df_jobs = delete_duplicates(df_jobs)
    df_jobs, df_requirements, df_jobs_and_requirements = transform_jobs_and_requirements(df_jobs)


def normalize_and_rename_job_offers(data: dict) -> pd.DataFrame:
    df_jobs = pd.json_normalize(
        data["postings"],
        sep="-"
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

def delete_duplicates(df_jobs: pd.DataFrame) -> pd.DataFrame
    df_jobs = df_jobs.drop_duplicates(subset=['reference'])

    return df_jobs

def transform_jobs_and_requirements(df_jobs: pd.DataFrame):
    # Create requirements mapping
    requirements_list = []
    job_requirements_list = []


    for _, row in df_jobs.iterrows():
        job_id = row['job_id']
        if isinstance(row['tiles_values'], list):
            for tile in row['tiles_values']:
                if tile['type'] == 'requirement':
                    requirement_name = tile['value']
                    requirements_list.append(requirement_name)
                    job_requirements_list.append({
                        'job_id': job_id,
                        'requirement_name': requirement_name
                    })

    # Create unique requirements with IDs
    df_requirements = pd.DataFrame({
        'requirement_id': range(1, len(set(requirements_list)) + 1),
        'requirement_name': list(set(requirements_list))
    })

    # Create job-requirements mapping
    df_job_requirements = pd.DataFrame(job_requirements_list)

    # Join with requirements to get requirement_ids
    df_job_requirements = df_job_requirements.merge(
        df_requirements,
        on='requirement_name'
    )[['job_id', 'requirement_id']]

    # Drop tiles.values from jobs dataframe as we don't need it anymore
    df_jobs = df_jobs.drop('tiles_values', axis=1)

    return df_jobs, df_requirements, df_job_requirements

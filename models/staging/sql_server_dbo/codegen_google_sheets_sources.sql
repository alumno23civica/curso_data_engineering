{{ codegen.generate_source('google_sheets') }}{{
    codegen.generate_source(
        schema_name = 'google_sheets',
        database_name = 'ALUMNO23_DEV_BRONZE_DB',
        table_names = ['budget'],
        generate_columns = True,
        include_descriptions=True,
        include_data_types=True,
        name='desarrollo',
        include_database=True,
        include_schema=True
        )
}}
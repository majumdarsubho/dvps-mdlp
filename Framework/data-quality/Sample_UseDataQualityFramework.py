# Databricks notebook source
# MAGIC %run "Data Quality/DataQualityFramework"

# COMMAND ----------

    data_val =  DataQualityFramework(raw_input_df,configure_file_df,converted_utc_df,global_parm_df)
    data_val.columns_validations()
    # record_validation_table_dict = data_val.columns_validations()
    print('column validations are done     :', datetime.datetime.now())

    # Data validations - row level
    final_df, validate_delta_dict = data_val.rows_data_validations()
    print('row validations are done        :', datetime.datetime.now())

    # Write record in column validation delta file
    error_details= {}
    error_details = {'Error' : 'None', 'code' : '0', 'row_count' : raw_input_df.count(),'col_count' : len(raw_input_df.columns)}
    write_record_in_validation_table(param_dict,validate_delta_dict,error_details)
    print ('validation delta file written   :' , datetime.datetime.now())

    # Overwrite the raw file with new fileds added file
    final_path = get_adls_path_and_filename(param_dict['finalfile_parm'])
    path = final_path.split(filename)[0]
    write_to_parquet(final_df, path, filename)
    print('new consolidate file path       :', final_path)
    print('new consolidate file written    :', datetime.datetime.now())

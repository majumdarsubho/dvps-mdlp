# Databricks notebook source
from pyspark.sql.functions import max as sparkMax
from pyspark.sql.functions import min as sparkMin
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col,isnan,when,count
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date as sprktoDate
from pyspark.sql.types import DecimalType, StringType, DoubleType
from dateutil import parser
from pyspark.sql.functions import col, round
import datetime
from pyspark.sql.functions import monotonically_increasing_id,row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import col,isnan,when,count, lit
from pyspark.sql import functions as sf

# COMMAND ----------


class DataQualityFramework:

    def __init__(self, raw_file_df, source_file_df,configure_file_df,converted_utc_df,global_parm_df):
        self.raw_file_df = raw_file_df
        self.source_file_df = source_file_df
        self.configure_file_df = configure_file_df
        self.converted_utc_df = converted_utc_df
        self.global_parm_df = global_parm_df
        self.str_min_max_dict = {}
        self.int_statistics_dict = {}
        self.unique_values_dict ={}
        self.date_min_max_dict = {}
        self.configure_flags_dict = {}
        self.errors_dict = {}
        self.MissingDataPercentage_dict ={}
        self.record_count = source_file_df.count()
        self.number_fields_list = []
        self.table_dict = {}
        return

    def columns_count_validation(self):
        # columns count validation 
        error_details = {} 
        self.record_validation_table_dict = {} 
        if len(self.source_file_df.columns)  != self.configure_file_df.count():
            error_details = {'Error' : 'Columns count is not matched', 'code' : '1',
                             'row_count' : self.source_file_df.count(),'col_count' : len(self.source_file_df.columns)}
            write_record_in_validation_table(param_dict,self.record_validation_table_dict,error_details)
            dbutils.notebook.exit('fail')
        return

    def coumns_datatype_validation(self):
        # columns datatype validation

        datatype_error_list = []
        error_details = {}
        for row in self.configure_file_df.collect():
            column_name = row['Column']
            if row['DataType'] !=  dict(self.source_file_df.dtypes)[column_name]:
                datatype_error_list.append(column_name)
   
        if datatype_error_list:
            error_details = {'Error' : 'Datatypes are not matched - ' + str(datatype_error_list), 'code' : '1' ,
                            'row_count' : self.source_file_df.count(),'col_count' : len(self.source_file_df.columns)}
            write_record_in_validation_table(param_dict,self.record_validation_table_dict,error_details)
            dbutils.notebook.exit('fail')
        return

    def get_tablemissing_percent(self):
        # calculate missing data percentage at table level

        table_missing_df = self.source_file_df.select([count(when(F.col(c).contains('None') | \
                                F.col(c).contains('NULL') | \
                                (F.col(c) == '' ) | \
                                F.col(c).isNull() , c 
                            ))for c in self.source_file_df.columns])

        table_missing_df = table_missing_df.withColumn('EmptyCount', sum([col(c) for c in table_missing_df.columns]))
        table_empty_percent = table_missing_df.select('EmptyCount').first()[0]/(self.source_file_df.count() * len(self.source_file_df.columns))*100
        return float("{:.2f}".format(table_empty_percent))

    def global_parms_into_dict(self):
        # get global paramters and values into dictionary

        return {row['ParameterName']: row['Value'] for row in self.global_parm_df.collect()}


    def min_max_valuelength(self,column):
        # get min and max length of the values of each column
 
        return ({column : {'Min' : self.source_file_df.agg(sparkMin(F.length(F.col(column)))).first()[0] ,
                                          'Max' : self.source_file_df.agg(sparkMax(F.length(F.col(column)))).first()[0]}})

    def unique_and_percentage(self):   
        # get unique values count of each column

        fields_list = []
        df_schema = list(self.source_file_df.schema)
#         print(df_schema)
        for x in df_schema:
            if ("DecimalType" in str(x)) or ("IntegerType" in str(x)) or ("StringType" in str(x)) or ("LongType" in str(x)) or ("DoubleType" in str(x)) :
                fields_list.append(str(x)[13:str(x).find("',")])
        
        unique_dict = {'TotalRecords' : self.record_count}
        unique_count = self.source_file_df.select([F.countDistinct (col(c)) for c in fields_list])
        unique_collect = unique_count.collect()
        unique_columns = unique_count.columns
        unique_dict.update({str(column)[15:str(column).find(")")]: [ 'unique :' + str(row[column]), 'percent :' +  str('%.3f'%((row[column]/self.record_count)*100)) + '%']  for row in unique_collect  for column in unique_columns })
        return unique_dict

    def min_max_dates(self):
        #get MinDate and Maxdate for each column

        date_fields_list = []
        min_dict = {}
        max_dict = {}
        min_max_dict = {}
        df_schema = list(self.source_file_df.schema)
        for x in df_schema:
            if ("TimestampType" in str(x)) or ("DateType" in str(x)):
                date_fields_list.append(str(x)[13:str(x).find("',")])
        min_df =  self.source_file_df.agg({c : 'min'  for c in date_fields_list})
        max_df = self.source_file_df.agg({c : 'max'  for c in date_fields_list})

        min_df_collect = min_df.collect()
        min_df_cols = min_df.columns
        max_df_collect = max_df.collect()
        max_df_cols = max_df.columns

        min_dict.update({str(column)[4:str(column).find(")")]: {'MinDate' :  str(row[column])} for row in min_df_collect for column in min_df_cols })
        max_dict.update({str(column)[4:str(column).find(")")]: {'MaxDate' :  str(row[column])} for row in max_df_collect for column in max_df_cols })
        min_max_dict.update({column: ['MinDate :' +  min_dict[column]['MinDate'] ,  'MaxDate :' + max_dict[column]['MaxDate'] ] for column in date_fields_list })

        return min_max_dict

    def get_flags_from_configurefile(self):
        # get validation csvs/local threshold values into dictionary

        return {row['Column']: {'MinDate' : row['MinDate'],'MaxDate' : row['MaxDate'] ,'StdDev' : row['StdDevColumn'] ,'MissingDataValue' : row['MissingPercentColumn'],
                               'Median' : row['Median'],'Percentile25' : row['Percentile25'] ,'Percentile50' : row['Percentile50'] ,'Percentile75' : row['Percentile75'],
                               'MinValueCount' : row['MinValueCount'],'MaxValueCount' : row['MaxValueCount'] ,'UniqueValueCount' : row['UniqueValueCount'] ,
                               'MinValueLength' : row['MinValueLength'],'MaxValueLength' : row['MaxValueLength'],'ShouldBeUnique' : row['ShouldBeUnique'] ,
                               'ShouldNotBeNull' : row['ShouldNotBeNull'],'ShouldMatchPattern' : row['ShouldMatchPattern'],'ShouldBeInList' : row['ShouldBeInList'],
                               'Include' : row['Include']
                               }
                                for row in self.configure_file_df.collect()}
    
    def get_empty_count_percentage(self):
        # calculate missing data percentage at columns level

        m_dict = {}
        empty_count_df = self.source_file_df.select([count(when(F.col(c).contains('None') | \
                            F.col(c).contains('NULL') | \
                            (F.col(c) == '' ) | \
                            F.col(c).isNull() , c 
                           ))for c in self.source_file_df.columns])
        empty_count_df_collect = empty_count_df.collect()
        empty_count_df_cols = list(empty_count_df.columns)
        m_dict.update({str(column)[28:str(column).find(",")] :  '%.3f'%(float(row[column]/self.record_count) * 100) + '%'
                        for row in empty_count_df_collect for column in empty_count_df_cols   
                        } )
        return m_dict
   
    def get_statistics_number(self):
        # calculate min,max,stddev,mean, median,percentile 25,50,75,minvaluecount and maxvaluecountfor all numeric columns

        df_schema = list(self.source_file_df.schema)
#         print(df_schema)
        for x in df_schema:
            if ("DecimalType" in str(x)) or ("IntegerType" in str(x)) or ("LongType" in str(x))  or ("DoubleType" in str(x)):
                self.number_fields_list.append(str(x)[13:str(x).find("',")])
        pecentile_df = self.source_file_df.select(self.number_fields_list)
        fields_desc_df = self.source_file_df.describe(self.number_fields_list)
        fields_desc_df_collect = fields_desc_df.collect()
        final_fields_cols = list(fields_desc_df.columns)
        del final_fields_cols[0]
        summary = {}
        number_statistics = {}

        if not self.number_fields_list:
            return number_statistics

        percentile_list = pecentile_df.approxQuantile(self.number_fields_list, [0.25,0.50,0.75], 0)
        percentile_dict = dict(zip(self.number_fields_list, percentile_list))

        for column in final_fields_cols:
            summary.update({column: {row['summary']: row[column] for row in fields_desc_df_collect}})
            if summary[column]['count'] == '0':
                number_statistics.update({column : {'Min' : None , 'Max' : None, 'Mean' : None, 'StdDev' : None, \
                                                'Percentile 25, 50, 75' : None ,'MinValueCount' : None , \
                                                'MaxValueCount' : None
                                                }})
            else:
                number_statistics.update({column : {'Min' : '%.3f'%(float(summary[column]['min'])) , 
                                                           'Max' : '%.3f'%(float(summary[column]['max'])), 
                                                           'Mean' : '%.3f'%(float(summary[column]['mean'])), 
                                                           'StdDev' : '%.3f'%(float(summary[column]['stddev'])) if summary[column]['stddev'] != None else None,
                                                           'Median' : percentile_dict[column][1],
                                                           'Percentile 25, 50, 75' : percentile_dict[column],
                                                           'MinValueCount' : pecentile_df.filter(pecentile_df[column] == summary[column]['min']).count(),
                                                           'MaxValueCount' : pecentile_df.filter(pecentile_df[column] == summary[column]['max']).count() 
                                                           
                                        }})
        return number_statistics

    def timestamp_validations(self):
        # validate dates against with global thresholds at row level

        t_row_df = self.source_file_df.select([(when((col(c) < parser.parse(self.global_parm_dict['MinDateTable']) ) |
                                                     (col(c) > parser.parse(self.global_parm_dict['MaxDateTable']) ) ,1 ).otherwise(0)
                                                )
                                                .alias(f"{c}_check1")  
                                                for c in self.source_file_df.columns
                                                if dict(self.source_file_df.dtypes)[c] in ('timestamp','date')
                                              ])

        return t_row_df

    def notnull_value_validations(self):
        # validation stddev for all numeric columns at row level   
       
        notnull_row_df = self.source_file_df.select([(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull(), 2).otherwise(0)
                           ).alias(f"{c}_check2")
                    for c in self.source_file_df.columns
                                                     if (self.configure_flags_dict[c]['ShouldNotBeNull'] == 1) | (self.configure_flags_dict[c]['ShouldNotBeNull'] == '1')])
    
        return notnull_row_df
    
    def unique_value_validations(self):
        # validation stddev for all numeric columns at row level 
        non_unique_dict = {}
#         print(self.configure_flags_dict)
        for column in self.source_file_df.columns:
            if (self.configure_flags_dict[column]['ShouldBeUnique'] == 1) | (self.configure_flags_dict[column]['ShouldBeUnique'] == '1') :                
                non_unique_list = source_file_df.groupBy(column).count().where("count > 1").rdd.map(lambda x: x[0]).collect()
                res = list(filter(lambda item: item is not None, non_unique_list))
                non_unique_dict.update({column : res})

#         print(non_unique_dict)
        unique_row_df = self.source_file_df.select([(when((col(c).isin(non_unique_dict[c]) ) , 3 ).otherwise(0)
                                                )
                                                .alias(f"{c}_check3")  
                                                for c in self.source_file_df.columns
                                                if (self.configure_flags_dict[c]['ShouldBeUnique'] == 1) | (self.configure_flags_dict[c]['ShouldBeUnique'] == '1')
                                              ])
#         display(unique_row_df)
        return unique_row_df

#     @udf("string")
#     def checkpattern_udf(value , column):
#         pattern = self.configure_flags_dict[column]['ShouldMatchPattern']  
#         print(pettern)
#         if re.match(pattern, str(value)) == None:
#             status = 'yes'
#         else:
#             status = 'no'
#         return status
#     def datapattern_validations(self):
#         # data pattern validations for the columns which are defined the pattern in validations csv file.        
#         datapattern_row_df = self.raw_file_df.select([(when((  (checkpattern_udf(col(c), c) == 'yes' ) ),5 ).otherwise(0)
#                                                 )
#                                                 .alias(f"{c}_check5")  
#                                                 for c in self.raw_file_df.columns
#                                                 if (self.configure_flags_dict[c]['ShouldMatchPattern'] != '') and (self.configure_flags_dict[c]['ShouldMatchPattern'] != 'null') and (self.configure_flags_dict[c]['Include'] == 'Y')
#                                               ])
        
#         display(datapattern_row_df)           
#         return datapattern_row_df

    def valueinlist_validations(self):
        # validation stddev for all numeric columns at row level 
             
        valueinlist_row_df = self.source_file_df.select([(when((col(c).isin(list(self.configure_flags_dict[c]['ShouldBeInList'].split("/"))) ) , 0 ).otherwise(4)
                                                )
                                                .alias(f"{c}_check4")  
                                                for c in self.source_file_df.columns
                                                if self.configure_flags_dict[c]['ShouldBeInList']
                                              ])
            
        return valueinlist_row_df
    
    def datapattern_validations(self):
        """data pattern validations for the columns which are defined the pattern in validations csv file."""
           
        datapattern_row_df = self.raw_file_df.select([(when(( re.compile(self.configure_flags_dict[c]['ShouldMatchPattern']).match(str(col(c)) ) == None  ),5 ).otherwise(0)
                                                )
                                                .alias(f"{c}_check5")  
                                                for c in self.raw_file_df.columns
                                                if (self.configure_flags_dict[c]['ShouldMatchPattern'] != '') and (self.configure_flags_dict[c]['ShouldMatchPattern'] != 'null') and (self.configure_flags_dict[c]['Include'] == 'Y')
                                              ])
        
        display(datapattern_row_df)
            
        return datapattern_row_df
    
    def create_results_dataframe(self, combined_df):

        print('create results df started       :', datetime.datetime.now())
        collect_data = combined_df.collect()
        check_lst = [c for c in combined_df.columns if "_check" in c]
        validate_list = []
        validate_dict = {}
        for row in collect_data:
#             print(row)
            date_cols = []
            unique_cols = []
            notnull_cols = []
            valueinlist_cols = []
            datapattern_cols = []
            for column in check_lst:
#                 print (column, row[column])
                org_col = column.split('_check')[0]
                if row[column] == 1:
                    date_cols.append(org_col)
                if row[column] == 2:
                    notnull_cols.append(org_col)
                if row[column] == 3:
                    unique_cols.append(org_col)
                if row[column] == 4:
                    valueinlist_cols.append(org_col)
                if row[column] == 5:
                    datapattern_cols.append(org_col)
                    
            validate_dict = {'Exceeded Dates Threshhold' : date_cols if date_cols else 'None' , 'Notnull Value Violation' : notnull_cols if notnull_cols else 'None' , 
                             'Unique Value Violation' : unique_cols  if unique_cols else 'None','ValueInList Violation' : valueinlist_cols if valueinlist_cols else 'None',
                            'Datapattern Violation' : datapattern_cols if datapattern_cols else 'None'}

            self.table_dict.update({'TimestampErrors' : 'yes' if date_cols else 'None'})
            validate_list.append(Row(validation_flag = str(validate_dict)))
        
        results = spark.createDataFrame(validate_list)
        return results

    def columns_validations(self):
        print('column validations started      :', datetime.datetime.now())
        self.columns_count_validation()
        print('coulmns count validation done   :', datetime.datetime.now())

#         self.coumns_datatype_validation()
#         print('coulmns datatype validation end :', datetime.datetime.now())

        self.global_parm_dict = self.global_parms_into_dict()
        print('global parm dict created        :', datetime.datetime.now())

        print ('missing data percent start      :', datetime.datetime.now())
        missingpecent_column = self.get_empty_count_percentage()
        print ('missing data percent end        :', datetime.datetime.now())

        tablemissingpercent =  self.get_tablemissing_percent()
        self.MissingDataPercentage_dict.update({'MissingPercentTable' : str(tablemissingpercent)+'%' , 
                                                'MissingPercentColumn' : missingpecent_column})

        if tablemissingpercent > float(self.global_parm_dict['MissingPecentTable'].split('%')[0]):
            error_details = {}
            record_validation_table_dict = {'MissingDataPercentage' :  self. MissingDataPercentage_dict if self.MissingDataPercentage_dict else None}
            error_details = {'Error' : ' MissingPecentTable is ' + self.global_parm_dict['MissingPecentTable'] + 
                             ' but found ' + str(tablemissingpercent) +'%', 'code' : '1' , 'row_count' : self.source_file_df.count(),
                             'col_count' : len(self.source_file_df.columns)}
            write_record_in_validation_table(param_dict,record_validation_table_dict,error_details)
            dbutils.notebook.exit('fail')

        print('Table missing percent done      :', datetime.datetime.now())

        self.configure_flags_dict = self.get_flags_from_configurefile()
        print ('csv flags in dictonary is done  :', datetime.datetime.now())
#         print (self.configure_flags_dict)

        print ('number statistics start         :' , datetime.datetime.now())
        self.int_statistics_dict =  self.get_statistics_number()
        print ('number statistics end           :' , datetime.datetime.now())
        
        print ('stddev validation start         :' , datetime.datetime.now())
        stddev_error = []       
        for column in self.int_statistics_dict:
            if (self.int_statistics_dict[column]['StdDev'] != None) \
               and (self.configure_flags_dict[column]['StdDev'] != None):
                if abs(float(self.int_statistics_dict[column]['StdDev'])) >  \
                    (float(self.global_parm_dict['StdDevTable']) * float(self.configure_flags_dict[column]['StdDev'])) :
                    stddev_error.append(column)           
        print ('stddev validation end           :' , datetime.datetime.now())

        print ('unique count & percent start    :' , datetime.datetime.now())
        self.unique_values_dict = self.unique_and_percentage() 
        print ('unique count & percent end      :' , datetime.datetime.now())
        
        print ('min and max timestamp  start    :' , datetime.datetime.now())        
        self.date_min_max_dict = (self.min_max_dates())
        print ('min and max timestamp  end      :' , datetime.datetime.now())  

        print('min and max string start        :' , datetime.datetime.now())

        for column in self.source_file_df.columns:
            if dict(self.source_file_df.dtypes)[column] == 'string':
                self.str_min_max_dict.update(self.min_max_valuelength(column)) 
        print('min and max string end          :' , datetime.datetime.now())

        self. table_dict = {'StatisticsPerNumericColumn' : self.int_statistics_dict  if self.int_statistics_dict else 'No numeric columns in table', 
                                        'MinMaxLengthsPerAlphanumericColumn' : self.str_min_max_dict , 
                                        'MinMaxDateTimesPerColumn' : self.date_min_max_dict if self.date_min_max_dict else 'No date columns in table',
                                        'UniqueValuesPerColumn' : self.unique_values_dict,
                                        'MissingDataPercentage' :  self. MissingDataPercentage_dict if self.MissingDataPercentage_dict else None,
                                        'StdDevErrors' : stddev_error if stddev_error else None}
        return self.table_dict

    def rows_data_validations(self):
        print('row validationa are started     :', datetime.datetime.now())
        
        timestamp_df = self.timestamp_validations()
        print('timestamp validation done       :', datetime.datetime.now())
        
        notnull_value_df = self.notnull_value_validations()
        print('notnull value validation done   :', datetime.datetime.now())
        
        unique_value_df = self.unique_value_validations()
        print('unique value validation done    :', datetime.datetime.now())
        
        valueinlist_df = self.valueinlist_validations()
        print('value in list validation done   :', datetime.datetime.now())
        
        datapattern_df = self.datapattern_validations()
        print('datapattern validation done     :' , datetime.datetime.now())
        
        timestamp_df =timestamp_df.withColumn("timestamp_row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))
        unique_value_df = unique_value_df.withColumn("unique_row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))
        notnull_value_df = notnull_value_df.withColumn("notnull_row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))
        valueinlist_df = valueinlist_df.withColumn("valueinlist_row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))
        datapattern_df = datapattern_df.withColumn("datapattern_row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))

        print(datapattern_df)  
        combined_df  = timestamp_df.join(unique_value_df,timestamp_df["timestamp_row_idx"] == unique_value_df["unique_row_idx"], 'LEFT' )\
                                   .join(notnull_value_df,timestamp_df["timestamp_row_idx"] == notnull_value_df["notnull_row_idx"], 'LEFT')\
                                   .join(valueinlist_df,timestamp_df["timestamp_row_idx"] == valueinlist_df["valueinlist_row_idx"], 'LEFT')\
                                   .join(datapattern_df,timestamp_df["timestamp_row_idx"] == datapattern_df["datapattern_row_idx"], 'LEFT')\
                                   .drop(*('timestamp_row_idx','unique_row_idx','notnull_row_idx','valueinlist_row_idx','datapattern_row_idx'))
                                    
        
#         display(combined_df)
        results_df = self.create_results_dataframe(combined_df)
        
#         results_df = self.create_results_dataframe(timestamp_df,unique_value_df)
        print ('results dataframe  ended        :', datetime.datetime.now())
        results_df= results_df.withColumn("row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))
        
#         display(results_df)
        
        self.converted_utc_df = self.converted_utc_df.withColumn("row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))
        joined_df  = self.converted_utc_df.join(results_df,["row_idx"]).drop("row_idx")
        print ('main dataframe join   ended     :', datetime.datetime.now())
        
        display(joined_df)
        return joined_df, self.table_dict


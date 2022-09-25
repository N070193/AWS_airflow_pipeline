from pyspark.sql.types import DecimalType,StringType
from pyspark.sql.functions import col,sha2,concat_ws
import json
from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException 
from delta import DeltaTable

spark = SparkSession.builder.appName('file_transformation_ml8').getOrCreate()

class Data_Transformation:
    def __init__(self):
        self.jsonData=self.read_cofiguration()
        self.actives_source = self.jsonData['actives_data']['raw_source']
        self.viewership_source = self.jsonData['viewership_data']['raw_source']
        self.actives_final_destination=self.jsonData['actives_data']['final_destination']
        self.viewership_final_destination=self.jsonData['viewership_data']['final_destination']
        self.transform1_columns_actives = self.jsonData['actives_data']['transform1_columns']
        self.transform2_columns_actives = self.jsonData['actives_data']['transform2_columns']
        self.transform1_columns_viewership = self.jsonData['viewership_data']['transform1_columns']
        self.transform2_columns_viewership = self.jsonData['viewership_data']['transform2_columns']
        self.masking_columns_actives=self.jsonData['actives_data']['masking_columns']
        self.masking_columns_viewership= self.jsonData['viewership_data']['masking_columns']        
        self.partition_columns_actives= self.jsonData['actives_data']['partition_columns']
        self.partition_columns_viewership=self.jsonData['viewership_data']['partition_columns']
        self.delta_location=self.jsonData["lookup-dataset"]["data-location"]
        self.pii_cols=self.jsonData["lookup-dataset"]["pii-cols"]
        
    #read config file of spark cluster:
    def read_cofiguration(self):
        app_config = spark.conf.get("spark.path")
        configData = spark.sparkContext.textFile(app_config).collect()
        data       = ''.join(configData)
        jsonData = json.loads(data)
        return jsonData
    
   #read the actives and viewership file 
    def read_file(self, path):
        df = spark.read.parquet(path)
        return df
    
    #write final transform data in stagging zone
    def write_final_file(self,df, path, partition_columns = []):
        df.write.mode("overwrite").partitionBy(partition_columns[0], partition_columns[1]).parquet(path)
    
    #transfrom the data(Converting the decimal with 7 precision)
    def transform1_columns(self,df,transform_columns):
        for column in transform_columns:
            df = df.withColumn(column,df[column].cast(DecimalType(10,7)))
        return df
    
    #transform the array into a comma-separated string
    def transform2_columns(self,df,transform_columns):
        for column in transform_columns:
            df = df.withColumn(column,concat_ws(",",col(column)))
        return df
    
    #masking the sensitive data(encryption)
    def masking_columns(self,df, masking_columns):
        for column in masking_columns:
            df = df.withColumn("encrypted_"+column,sha2(col(column),256))
        return df

    #scd2 implementation
    def lookup_table(self,df, delta_location, pii_cols,filename):
    
        source_data =df.withColumn("begin_date", f.current_date())
        source_data = source_data.withColumn("update_date", f.lit("null"))
        flag = False
        columns= []
        for col in pii_cols:
            if col in source_data.columns:
                columns += [col, "encrypted_" + col]
        source_columns = columns + ['begin_date', 'update_date']
    
        source_data = source_data.select(source_columns)
    
        try:
            Target_Delta_Table = DeltaTable.forPath(spark,delta_location+filename)
            delta_df=Target_Delta_Table.toDF()
            delta_df.show()
        except AnalysisException:
            print('Delta Table not found')
            source_data = source_data.withColumn("active_flag",f.lit("true"))
            source_data.write.format("delta").save(delta_location+filename)
            print('Delta Table Created')
            Target_Delta_Table = DeltaTable.forPath(spark,delta_location+filename)
            delta_df = targetTable.toDF()
            delta_df.show()
    
        
        insert_col_dict={x: "updates." + x for x in columns}
        insert_col_dict['begin_date'] = f.current_date()
        insert_col_dict['active_flag'] = "true" 
        insert_col_dict['update_date'] = "null"
    
        delta_df = delta_df.select(*(f.col(x).alias(filename + x) for x in delta_df.columns))
        Existing_data=source_data.join(delta_df, source_data["advertising_id"] == delta_df[filename+"advertising_id"], "leftouter")
        Existing_data.show()
    
        New_data = Existing_data.filter(f"{filename+'advertising_id'} is null")
        filter_df = Existing_data.filter(Existing_data['advertising_id'] != Existing_data[filename+"advertising_id"])
    
        filter_df = filter_df.union(New_data)
        print("Fetching new & updated records")
        filter_df.show()
    
        if filter_df.count() != 0:
            if file_name == "Viewership":
                merge_data = filter_df.withColumn("MERGEKEY", filter_df[filename+"advertising_id"])
            else:
                merge_data = filter_df.withColumn("MERGEKEY", f.concat(filter_df[filename+"advertising_id"], filter_df[filename+"user_id"]))
        
            df1 = filter_df.filter(f"{filename+'advertising_id'} is null").withColumn("MERGEKEY", f.lit(None))
            slow_changing_df = merge_data.union(df1)
            slow_changing_df.show()
        
            if file_name == "Viewership":
                Target_Delta_Table.alias(filename).merge(
                    source= slow_changing_df.alias("source"),
                    condition=f"{filename}.advertising_id = source.MERGEKEY and {filename}.flag_active = 'true'"
                ).whenMatchedUpdate(set={
                    "update_date": f.current_date(),
                    "flag_active": "False",
                }).whenNotMatchedInsert(values=insert_col_dict
                                        ).execute()
                flag = True
            
            else:
                Target_Delta_Table.alias(filename).merge(
                    source=slow_changing_df.alias("source"),
                    condition=f"concat({filename}.advertising_id, {filename}.user_id) = source.MERGEKEY and {filename}.flag_active = 'true'"
                ).whenMatchedUpdate(set={
                    "update_date": f.current_date(),
                    "flag_active": "False",
                }).whenNotMatchedInsert(values=insert_col_dict
                                        ).execute()
                flag = True
            
            if flag:
                print("Successfully updated delta table")
            else:
                print("No changes made in delta table")
        
        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("encrypted_" + i, i)
        
        return df
        
        
T = Data_Transformation()
filename=sys.argv[1]

if filename == "Actives":
    actives_df = T.read_file(T.actives_source)
    actives_masked_data =T.masking_columns(actives_df,T.masking_columns_actives)
    actives_tranform1_data = T.transform1_columns(actives_masked_data, T.transform1_columns_actives)
    actives_tranform2_data = T.transform2_columns(actives_tranform1_data, T.transform2_columns_actives)
    dfActives = T.lookup_table(actives_tranform2_data, T.delta_location, T.pii_cols,filename)
    T.write_final_file(dfActives ,T.actives_final_destination,T.partition_columns_actives)
    
elif filename == "Viewership":
    viewership_df = T.read_file(T.viewership_source)
    viewership_masked_data =T.masking_columns(viewership_df,T.masking_columns_viewership)
    viewership_tranform1_data = T.transform1_columns(viewership_masked_data, T.transform1_columns_viewership)
    viewership_tranform2_data = T.transform2_columns(viewership_tranform1_data, T.transform2_columns_viewership)
    dfVieweship = T.lookup_table(viewership_tranform2_data, T.delta_location, T.pii_cols,filename)
    T.write_final_file(dfVieweship ,T.viewership_final_destination,T.partition_columns_viewership)

else:
    pass

spark.stop()
    
    
        
    
    
    
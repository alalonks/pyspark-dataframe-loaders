from pyspark.sql import SparkSession, DataFrame


class FileLoader:
    spark=None
    file_type = "csv"
    infer_schema = "false"
    first_row_is_header = "true"
    delimiter = ","
    
    def __init__(self, spark_session, **kwargs):
        
        self._process_arg(spark_session, kwargs)
    
    def _handleException(func):
        def wrapper(*args):
            try:
                func(*args)
            except KeyError as ke:
                print('Key Does Not Exist! ')
                print(ke)
            except Exception as e:
                print('There was some sort of error!') 
                print(e)
        return wrapper
    
    
    @_handleException
    def _process_arg(self, spark_session, dict_of_args):
        if spark_session != None:
            self.spark = spark_session
            print("Using provide spark_session")
        else:
            self.spark = SparkSession.builder.appName("myApp").getOrCreate()
            print("Using self generated spark_session")
        if bool(dict_of_args): 
            self.file_type =  dict_of_args['dict_of_args']
            self.infer_schema = dict_of_args['infer_schema']
            self.first_row_is_header = dict_of_args['first_row_is_header'] 
            self.delimiter = dict_of_args['delimiter']
        

    def spark_load(this, uri_location):
        print("file_type is " + FileLoader.file_type)
        print("infer_schema is  " + FileLoader.infer_schema)
        print("first_row_is_header is " + FileLoader.first_row_is_header)
        print("delimiter is " + FileLoader.delimiter)
        
        df = this.spark.read.format(this.file_type) \
            .option("inferSchema", this.infer_schema) \
                .option("header", this.first_row_is_header) \
                    .option("sep", this.delimiter) \
                        .load(uri_location)
        
        return df 
        


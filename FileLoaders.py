from pyspark.sql import SparkSession

class FileLoader:
    spark=None
    file_type = "csv"
    infer_schema = "false"
    first_row_is_header = "true"
    delimiter = ","
    
    def __init__(self, spark_session, **kwargs):
        
        self._process_args(self,spark_session, kwargs)
    
    def _handleException(func):
        def wrapper(*args):
            try:
                func(*args)
            except KeyError as e:
                print('Key Does Not Exist! ' + e)
            except Exception:
                print('There was some sort of error!')
        return wrapper
    
    
    @_handleException
    def _process_arg(self, spark_session, dict_of_args):
        if spark_session != None:
            spark = spark_session
        else:
            spark = SparkSession.builder.appName("myApp").getOrCreate()
        
        self.file_type =  dict_of_args['dict_of_args']
        self.infer_schema = dict_of_args['infer_schema']
        self.first_row_is_header = dict_of_args['first_row_is_header'] 
        self.delimiter = dict_of_args['delimiter']

    @staticmethod
    def spark_load(uri_location):
        df = FileLoader.spark.read.format("lala")
        return df
        


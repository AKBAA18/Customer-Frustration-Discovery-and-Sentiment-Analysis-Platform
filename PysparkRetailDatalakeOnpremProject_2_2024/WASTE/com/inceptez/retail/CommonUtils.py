from pyspark.sql import SparkSession
from pyspark.sql import *
#pip install delta-spark==1.0.1
from pyspark import SparkConf
from configparser import *
import sys
from delta.tables import *
from delta import *
from pyspark.sql.functions import *

#app.properties
#[CONFIGS]
#spark.app.name=spark_app_win10
#spark.master=local[1]
def get_spark_session(typeflg,propfile,jdbc_lib):
    spark_config = SparkConf()
    config = ConfigParser()
    config.read(propfile)
    for config_name, config_value in config.items("CONFIGS"):
        spark_config.set(config_name, config_value)

    # creating Spark Session variable
    try:
      if typeflg=='delta':
        #builder= SparkSession.builder.config(conf=spark_config).config("spark.jars",jdbc_lib)
        #spark = configure_spark_with_delta_pip(builder).getOrCreate()
        #return spark
        builder = SparkSession.builder.appName("MyApp") \
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
          .config("spark.jars", jdbc_lib).enableHiveSupport()
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark
      else:
        spark = SparkSession.builder.config(conf=spark_config).config("spark.jars", jdbc_lib).enableHiveSupport().getOrCreate()
        return spark

    except Exception as spark_error:
        print(spark_error)
        sys.exit(1)

def read_data(type,sparksess,src,strtype,infersch=False,delim=',',headerflag=False): # reusable framework
    if type=='csv':
     df1=sparksess.read.csv(src, header=headerflag, inferSchema=infersch, sep=delim)
     return df1
    elif type=='json':
     df1 = sparksess.read.option("multiline", "true").schema(strtype).json(src)
     #df1.select(col("pagevisit").getItem(0)).show()
     #df1=sparksess.read.json(src)
     return df1

def write_data(type,df,tgt,mode,delim=',',headerflag=False):
    if type=='csv':
     df.write.mode(mode).csv(tgt, header=headerflag, sep=delim)
    elif type=='json':
     df.write.mode(mode).option("multiline", "true").json(tgt)


def checkForHiveTable(sparksess, dbname, tblname):
    from pyspark.sql.functions import col
    if (sparksess.sql(f"show tables in {dbname}").filter(col("tableName") == tblname).count() > 0):
        return True
    else:
        return False

def readHiveTable(sparksess, TableName):
     hivedf=sparksess.read.table(TableName)
     return hivedf

def writeHiveTable(df, tblname, partflag, partcols,mode):
 if partflag == False:
    df.write.mode(mode).saveAsTable(tblname)
 else:
    df.write.mode(mode).partitionBy(partcols).saveAsTable(tblname)

def getRdbmsPartData(propfile,sparksess,db,tbl,partcol,lowerbound,upperbound,numpart):
    config = ConfigParser()
    config.read(propfile)
    driver=config.get("DBCRED", 'driver')
    host=config.get("DBCRED", 'host')
    port=config.get("DBCRED", 'port')
    user=config.get("DBCRED", 'user')
    passwd=config.get("DBCRED", 'pass')
    url=host+":"+port+"/"+db

    db_df=sparksess.read.format("jdbc").option("url",url)\
    .option("dbtable",tbl)\
    .option("user",user).option("password",passwd)\
    .option("driver",driver) \
    .option("lowerBound", lowerbound)\
    .option("upperBound", upperbound)\
    .option("numPartitions", numpart)\
    .option("partitionColumn", partcol)\
    .load()
    return db_df

def getRdbmsData(propfile,sparksess,db,tbl):#configuration driven approach
    config = ConfigParser()
    config.read(propfile)
    driver=config.get("DBCRED", 'driver')
    host=config.get("DBCRED", 'host')
    port=config.get("DBCRED", 'port')
    user=config.get("DBCRED", 'user')
    passwd=config.get("DBCRED", 'pass')
    url=host+":"+port+"/"+db
    #jdbc:mysql://127.0.0.1:3307/empoffice
    db_df=sparksess.read.format("jdbc").option("url",url)\
    .option("dbtable",tbl)\
    .option("user",user).option("password",passwd)\
    .option("driver",driver) \
    .load()
    return db_df

def writeRDBMSData(df,propfile,db,tbl,mode):
    config = ConfigParser()
    config.read(propfile)
    driver=config.get("DBCRED", 'driver')
    host=config.get("DBCRED", 'host')
    port=config.get("DBCRED", 'port')
    user=config.get("DBCRED", 'user')
    passwd=config.get("DBCRED", 'pass')
    url=host+":"+port+"/"+db
    url1 = url+"?user="+user+"&password="+passwd
    df.write.jdbc(url=url1, table=tbl, mode=mode)


def optimize_performance(sparksess,df,numpart,partflag,cacheflag,numshufflepart=200):
    print("Number of partitions in the given DF {}".format(df.rdd.getNumPartitions()))
    if partflag:
     df = df.repartition(numpart)
     print("repartitioned to {}".format(df.rdd.getNumPartitions()))
    else:
     df = df.coalesce(numpart)
     print("coalesced to {}".format(df.rdd.getNumPartitions()))

    if cacheflag:
     df.cache()
     print("cached ")

    if numshufflepart!=200:
     # default partions to 200 after shuffle happens because of some wide transformation spark sql uses in the background
     sparksess.conf.set("spark.sql.shuffle.partitions", numshufflepart)
     print("Shuffle part to {}".format(numshufflepart))
    return df

def munge_data(df,dedupflag,naallflag,naanyflag,naallsubsetflag,naanysubsetflag,sub):
    print("Number of partitions in the given DF {}".format(df.rdd.getNumPartitions()))
    if dedupflag:
     print("Raw df count is {} ".format(df.count()))
     df = df.dropDuplicates()
     print("deduplicated count is {} ".format(df.count()))

    if naallflag:
     df=df.na.drop("all")

     if naanyflag:
      df = df.na.drop("any")

     if naallsubsetflag:
         df = df.na.drop("all",subset=sub)

     if naanysubsetflag:
         df = df.na.drop("any",subset=(sub))
     print("count of df is {}".format(df.count()))
     return df
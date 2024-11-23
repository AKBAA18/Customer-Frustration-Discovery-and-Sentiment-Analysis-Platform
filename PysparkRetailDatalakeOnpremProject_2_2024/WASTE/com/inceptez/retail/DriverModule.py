#https://docs.delta.io/latest/quick-start.html
#Prerequisite:
#pip install delta-spark==1.0.1
#goto Python Console and run the below code:
#import shutil
#shutil.make_archive("/home/hduser/PysparkRetailDatalakeProject3_2022/common_functions1","zip", root_dir="/home/hduser/PycharmProjects/RetailProject/")
#Goto command line and run the spark-submit command
#spark-submit --packages io.delta:delta-core_2.12:1.0.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --py-files /home/hduser/PysparkRetailDatalakeProject3_2022/common_functions.zip DriverModule.py /home/hduser/PysparkRetailDatalakeProject3_2022/connection.prop /home/hduser/install/mysql-connector-java.jar /home/hduser/PysparkRetailDatalakeProject3_2022/app.properties
from delta import *
from pyspark.sql.functions import *

def main(arg1_conn_file,arg2_conn_jar,arg3_spark_sess):
    print("################################# Create spark session #########################")
    spark = get_spark_session('delta',arg3_spark_sess,arg2_conn_jar)
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("drop database if exists retail_curated cascade");
    spark.sql("create database retail_curated");
    spark.sql("drop database if exists retail_discovery cascade");
    spark.sql("create database retail_discovery");
    #spark.sql("drop database if exists retail_dim cascade");
    spark.sql("create database if not exists retail_dim");

    print("#################################Employees Data (Slowly Changing Dimension 2) #########################")
    print("################################# Select only the employees info updated or inserted today in the source DB#########################")
    empquery="(select * from employees where upddt>current_date-interval 1 day) tblquery"#CDC/Incremental data ingestion
    df_employees_new_updated=getRdbmsData(arg1_conn_file,spark,'empoffice',empquery)
    print("Employee RDBMS Data")
    df_employees_new_updated.show(2)
    print("Data munging")
    df_employees_new_updated=munge_data(df_employees_new_updated,True,True,False,False,False,"")
    print("Check for hive table")
    tableexistflag=checkForHiveTable(spark,"retail_dim","hive_employees")
    print(tableexistflag)
    print("SCD2 data load to hive table retail_dim.hive_employees")
    writeHiveTableSCD2(tableexistflag, spark,"retail_dim.hive_employees",df_employees_new_updated)

    print("################################# Select entire Office RDBMS Data #########################")
    df_offices=getRdbmsData(arg1_conn_file,spark,'empoffice',"offices")
    print("Data munging")
    df_offices = munge_data(df_offices, True, True, False, False, False, "")
    print("Overwrite Hive table retail_dim.offices_raw")
    writeHiveTable(df_offices, "retail_dim.offices_raw", False, "","overwrite")

    print("################################# Select joined Customer & Payments Data (Apply Partition and Pushdown Optimization) #########################")
    custpaymentsquery = """(select c.customerNumber customernumber, upper(c.customerName) as custname,
    c.country ,c.salesRepEmployeeNumber,c.creditLimit ,
    p.checknumber,p.paymentdate,p.amount,current_date as datadt  
    from customers c inner join payments p 
    on c.customernumber=p.customernumber 
    and year(p.paymentdate)=2022
    and month(p.paymentdate)>=07) query """

    dfcustpayments = getRdbmsPartData(arg1_conn_file,spark,'custpayments',custpaymentsquery,"customernumber",1,100,4)
    print("################################# optimize_performance of the DF #########################")
    dfcustpayments = optimize_performance(spark, dfcustpayments, 4, True, True, 10)
    print("Overwrite Hive table retail_curated.custpayments")
    writeHiveTable(dfcustpayments, "retail_curated.custpayments", True, "datadt","overwrite")
    spark.sql("select * from retail_curated.custpayments ").show(2)
    dfcustpayments.createOrReplaceTempView("custpayments")

    print("################################# Orders & orderdetails Data #########################")

    orderdetailsquery="""(select o.customernumber,o.ordernumber,o.orderdate,o.shippeddate,o.status,o.comments,
      od.quantityordered,od.priceeach,od.orderlinenumber,od.productCode,current_date as datadt
      from orders o inner join orderdetails od 
      on o.ordernumber=od.ordernumber  
      and year(o.orderdate)=2022
      and month(o.orderdate)>=07 ) orders"""

    dforderdetailsquery = getRdbmsPartData(arg1_conn_file,spark,"ordersproducts",orderdetailsquery,"customernumber",1,100,4)
    dforderdetailsquery=optimize_performance(spark,dforderdetailsquery,5,True,True,10)
    dforderdetailsquery.show(2)
    #writeHiveTable(dforderdetailsquery, "retail_curated.orderdetails", True, "datadt","overwrite")

    print("################################# Products Data (Apply Delta lake merge to update if products exists else insert) #########################")
    print("################################# Product Profit value, Profit percent, promotion indicator and demand indicator KPI Metrics #########################")
    prodquery="(select * from products where upddt>current_date-interval 1 day) tblquery"
    df_products_new = getRdbmsData(arg1_conn_file, spark, 'ordersproducts',prodquery)
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, FloatType,DecimalType
    returnstructure = StructType([StructField("profit", DecimalType(10,2), True),StructField("profitpercent", DecimalType(10,2), True),StructField("promoind",ShortType(),True),StructField("demandind",ShortType(),True)])
    udfDFProfPromo=udf(udfProfPromo,returnstructure)
    df_products_new_udf=df_products_new.select("productCode","productName","productLine","productScale","productVendor",
                                               "productDescription","quantityInStock","buyPrice","MSRP","upddt",
                                               udfDFProfPromo(col("buyPrice"),col("MSRP"),col("quantityInStock")).alias("profPromo"))
    df_products_new_udf.printSchema()
    df_products_new_udf=df_products_new_udf.select("productCode","productName","productLine","productScale","productVendor",
                                               "productDescription","quantityInStock","buyPrice","MSRP","upddt",
                                               "profPromo.profit","profPromo.profitpercent","profPromo.promoind","profPromo.demandind")
    print("UDF applied Products with Profit ratio, profit percent, promo indicator and demand indicator")
    df_products_new_udf.show(3)
    tableexistflag=checkForHiveTable(spark,"retail_dim","hive_products")
    print(tableexistflag)
    print("(Apply Delta lake merge to update if products exists else insert into retail_dim.hive_products)")
    df_product_merged=writeHiveTableDeltaMerge(tableexistflag, spark, "retail_dim.hive_products", df_products_new_udf)

    print("################################# Data Wrangling ie joining of merged latest products with the orders #########################")
    df_orders_products=wrangle_data(df_product_merged,dforderdetailsquery)
    df_orders_products.show(5,False)
    print("Overwrite Hive table retail_curated.ordersproducts")
    writeHiveTable(df_orders_products, "retail_curated.ordersproducts", True, "datadt", "overwrite")

    print("################################# Custnavigation nested json Data parsing with custom schema #########################")

    strtype = StructType([StructField("id", StringType(), False), StructField("comments", StringType(), True),
                          StructField("pagevisit", ArrayType(StringType()), True)])

    print("Read Cust navigation json data")
    df_cust_navigation=read_data("json",spark,"file:///home/hduser/PysparkRetailDatalakeProject3_2022/cust_navigation.json",strtype)
    df_cust_navigation.createOrReplaceTempView("cust_navigation")
    #df_cust_navigation.show(2)

    print("################# Customer Navigation Curation load to hive with positional explode #########################")
    print("Create hive external table using hql, insert into the table and write the curated data into json")
    spark.sql("""create external table if not exists retail_curated.cust_navigation 
    (customernumber string,navigation_index int,navigation_pg string) 
    row format delimited fields terminated by ',' 
    location '/user/hduser/custnavigation/'""")

    spark.sql("""insert into table retail_curated.cust_navigation 
    select id,pgnavigationidx,pgnavigation  from cust_navigation 
    lateral view posexplode(pagevisit) exploded_data1 as pgnavigationidx,pgnavigation""")

    spark.sql("""select id,pgnavigationidx,pgnavigation  
    from cust_navigation lateral view posexplode(pagevisit) exploded_data1 as pgnavigationidx ,pgnavigation""").show(4,False)

    report1=spark.sql("""select c1.navigation_pg,count(distinct c1.customernumber) custcnt,'last pagevisited' pagevisit 
    from retail_curated.cust_navigation  c1 inner join (select a.customernumber,max(a.navigation_index) as maxnavigation 
    from retail_curated.cust_navigation a group by a.customernumber) as c2 
    on (c1.customernumber=c2.customernumber and c1.navigation_index=c2.maxnavigation) 
    group by c1.navigation_pg
    union all
    select navigation_pg,count(distinct customernumber) custcnt,'first pagevisited' pagevisit
    from retail_curated.cust_navigation
    where navigation_index=0
    group by navigation_pg""");
    curdt = spark.sql("select date_format(current_date(),'yyyyMMdd')").first()[0]
    write_data("json",report1.coalesce(1),"hdfs://localhost:54310/user/hduser/retail_discovery/first_last_page"+curdt,"overwrite");
    report1.show(4,False)

    print("################# Dim Order table External table load using load command #########################")
    spark.sql("""create external table if not exists retail_discovery.dim_order_rate (rid int,orddesc varchar(200),
    comp_cust varchar(10),siverity int, intent varchar(100)) 
    row format delimited fields terminated by ','
    location '/user/hduser/dimorders/'""")

    spark.sql("""load data local inpath 'file:///home/hduser/PysparkRetailDatalakeProject3_2022/orders_rate.csv' 
        overwrite into table retail_discovery.dim_order_rate""")

    print("################# Employee Rewards Discovery load #########################")
    df_offices.createOrReplaceTempView("offices_raw_view")
    spark.sql(
        "select o.*,e.*,c.* from offices_raw_view o "
        "inner join retail_dim.hive_employees e "
        "inner join custpayments c "
        "on o.officecode=e.officecode "
        "and e.employeenumber=c.salesrepemployeenumber").createOrReplaceTempView("office_emp_custpayments")

    df_emp_rewards=spark.sql("select * from (select email,sumamt,rank() over(partition by state order by sumamt desc) as rnk,"
                             "current_date as datadt "
                             "from "
              "( select state,email,sum(amount) sumamt from office_emp_custpayments "
              "where datadt>=date_add(current_date,0) "
              "group by email,state) temp ) temp2 where rnk=1")
    df_emp_rewards.show(4,False)
    print("Overwrite Hive table retail_discovery.employeerewards to send the rewards to high performing employees")

    writeHiveTable(df_emp_rewards, "retail_discovery.employeerewards", True, "datadt", "overwrite")

    print("################# Customer Frustration Discovery joining cust_navigation and retail_dim.dim_order_rate tables #########################")
    frustrateddf = spark.sql(""" select customernumber,
      total_siverity,case when total_siverity between -10 and -3 then 'highly frustrated' when total_siverity 
      between -2 and -1 then 'low frustrated' when total_siverity = 0 then 'neutral' 
      when total_siverity between 1 and 2 then 'happy' when total_siverity between 3 and 10 
      then 'overwhelming' else 'unknown' end as customer_frustration_level from 
      ( select customernumber,sum(siverity) as total_siverity from 
      ( select o.id as customernumber,o.comments,r.orddesc,siverity 
      from cust_navigation o left outer join retail_discovery.dim_order_rate r  
      where o.comments like concat('%',r.orddesc,'%')) temp1 
    group by customernumber) temp2""")
    frustrateddf.show(3,False)

    print("################# Convert DF to frustrationview tempview #########################")
    frustrateddf.createOrReplaceTempView("frustrationview")

    print("Overwrite Hive table retail_discovery.cust_frustration_level to store the customer frustration levels")

    spark.sql("""create external table if not exists retail_discovery.cust_frustration_level 
      (customernumber string,total_siverity int,frustration_level string) 
      row format delimited fields terminated by ',' 
      location '/user/hduser/custmartfrustration/'""")

    print("Writing Customer frustration data into Hive")
    spark.sql("""insert overwrite table retail_discovery.cust_frustration_level select * from frustrationview""")

    print("Writing Customer frustration data into RDBMS")

    writeRDBMSData(frustrateddf, arg1_conn_file, "custdb","customer_frustration_level","overwrite")
    print("Writing Customer frustration data into S3 Bucket Location")
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA3YF5CC3LQDFLDUMU")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    #write_data('csv',frustrateddf,"s3a://com.iz.test1/IZ_datalake/",'append',',',True)
    print("################################# Application completed successfully #########################")
if __name__ == '__main__':
   print("################################# Starting the main application #########################")
   import sys
   if len(sys.argv) == 4:
      print("################################# Initializing the Dependent modules #########################")
      from com.inceptez.retail.CommonUtils import read_data,get_spark_session,getRdbmsPartData,getRdbmsData,\
          checkForHiveTable,munge_data,writeHiveTable,write_data,writeRDBMSData,optimize_performance
      from com.inceptez.retail.ETLProcess import writeHiveTableSCD2,writeHiveTableDeltaMerge,wrangle_data,udfProfPromo
      from pyspark.sql.types import *
      print("Calling the main method with {0}, {1}, {2},{3}".format(sys.argv[0],sys.argv[1],sys.argv[2],sys.argv[3]))
      main(sys.argv[1],sys.argv[2],sys.argv[3])
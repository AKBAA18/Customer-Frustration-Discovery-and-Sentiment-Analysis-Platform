from pyspark.sql.functions import *
from pyspark.sql.window import Window
from com.inceptez.retail.CommonUtils import readHiveTable
from delta.tables import *
from delta import *

def writeHiveTableSCD2(tableexistflag,sparksess,hive_table,df_employees_new_updated):
    if tableexistflag:
        print("hive_employees table exists")
        df_hive_existing = readHiveTable(sparksess,hive_table).groupBy("employeeNumber").agg(max("ver").alias("max_ver"))
        if df_hive_existing.count() > 0:
            joined_df = df_employees_new_updated.alias("new").join(df_hive_existing.alias("exist"), on="employeeNumber",
                                                                   how="left")#lookup and enrichment (Data wrangling)
            joined_df_exist = joined_df \
                .select("new.employeeNumber", "new.lastName", "new.firstName", "new.extension",
                        "new.email", "new.officeCode", "new.reportsTo", "new.jobTitle", "new.upddt", "new.leaveflag",
                        "exist.max_ver") \
                .withColumn("ver", expr(
                "row_number() over(partition by employeeNumber order by upddt) + coalesce(max_ver,0)")).drop("max_ver")
            #                .where("new.employeeNumber is not null")\
            joined_df_exist.show(10)
            #writeToHiveTable(joined_df_exist,"Append","hive",hive_table)
            joined_df_exist.createOrReplaceTempView("empnewexist")
            print("employee exist and the old+new data is..")
            sparksess.sql("select * from empnewexist").show(5, False)
            sparksess.sql("insert into retail_dim.hive_employees select * from empnewexist")
    else:
        df_employees_new_updated=df_employees_new_updated.withColumn("ver", lit(1))#Data Enrichment
        df_employees_new_updated.createOrReplaceTempView("empnew")
        print("employee table not exist and the new data is..")
        sparksess.sql("select * from empnew").show(5,False)
        sparksess.sql("""CREATE external TABLE retail_dim.hive_employees(
        `employeenumber` int,`lastname` string,`firstname` string,`extension` string,`email` string, 
        `officecode` string, `reportsto` int,`jobtitle` string, `upddt` date, `leaveflag` string,`ver` int)
        row format delimited fields terminated by ','
        location '/user/hduser/empdata/'""")
        sparksess.sql("insert into retail_dim.hive_employees select * from empnew")
        #writeToHiveTable(df_employees_new_updated, "Overwrite", "hive", hive_table)
        print("table doesn't exists, hence created and loaded the data")

def writeHiveTableDeltaMerge(tableexistflag,sparksess,hive_table,df_products_new):
    if tableexistflag:
        print("table exists")
        df_hive_existing=readHiveTable(sparksess,hive_table)
        df_hive_existing.write.format("delta").mode('overwrite').save("/user/hduser/df_hive_existing")
        exist_df = DeltaTable.forPath(sparksess, "/user/hduser/df_hive_existing")
        exist_df.alias("exist").merge(
            source=df_products_new.alias("new"),
            condition=expr(
                "new.productCode=exist.productCode")
        ).whenMatchedUpdate(set=
        {
            "productName": col("new.productName"),
            "productLine": col("new.productLine"),
            "productScale": col("new.productScale"),
            "productVendor": col("new.productVendor"),
            "productDescription": col("new.productDescription"),
            "quantityInStock": col("new.quantityInStock"),
            "buyPrice": col("new.buyPrice"),
            "MSRP": col("new.MSRP"),
            "upddt": col("new.upddt"),
            "profit": col("new.profit"),
            "profitpercent": col("new.profitpercent"),
            "promoind": col("new.promoind"),
            "demandind": col("new.demandind")
        }
        ).whenNotMatchedInsert(values=
        {
            "productCode": col("new.productCode"),
            "productName": col("new.productName"),
            "productLine": col("new.productLine"),
            "productScale": col("new.productScale"),
            "productVendor": col("new.productVendor"),
            "productDescription": col("new.productDescription"),
            "quantityInStock": col("new.quantityInStock"),
            "buyPrice": col("new.buyPrice"),
            "MSRP": col("new.MSRP"),
            "upddt": col("new.upddt"),
            "profit": col("new.profit"),
            "profitpercent": col("new.profitpercent"),
            "promoind": col("new.promoind"),
            "demandind": col("new.demandind")
        }
        ).execute()
        complete_df=exist_df.toDF()
        complete_df.registerTempTable("completeTable")
        sparksess.sql("drop table if exists retail_dim.hive_products")
        sparksess.sql(""" create table if not exists retail_dim.hive_products (productcode string,productname string,
        productline string, productscale string, productvendor string, productdescription string,
        quantityinstock int,buyprice decimal(10,2),msrp decimal(10,2),upddt date,
        profit decimal(10,2),profitpercent decimal(10,2),promoind smallint,demandind smallint )""")
        sparksess.sql(""" insert overwrite table retail_dim.hive_products select * from completeTable""")
        return complete_df
    else:
        print("table doesn't exists")
        df_products_new.registerTempTable("completeTable")
        sparksess.sql("drop table if exists retail_dim.hive_products")
        sparksess.sql(""" create table if not exists retail_dim.hive_products (productcode string,productname string,
        productline string, productscale string, productvendor string, productdescription string,
        quantityinstock int,buyprice decimal(10,2),msrp decimal(10,2),upddt date,
        profit decimal(10,2),profitpercent decimal(10,2),promoind smallint,demandind smallint) """)
        sparksess.sql(""" insert into table retail_dim.hive_products select * from completeTable""")
        return df_products_new

orderprodquery = """(select o.customernumber,o.ordernumber,o.orderdate,o.shippeddate,o.status,o.comments,
      od.quantityordered,od.priceeach,od.orderlinenumber,p.productCode,p.productName,
      p.productLine,p.productScale,p.productVendor,p.productDescription,p.quantityInStock,p.buyPrice,p.MSRP  
      from orders o inner join orderdetails od 
      on o.ordernumber=od.ordernumber  
      inner join products p 
      on od.productCode=p.productCode 
      and year(o.orderdate)=2022 
      and month(o.orderdate)=10 ) ordprod"""

def wrangle_data(df_product_merged,dforderdetailsquery):
    df=df_product_merged.alias("p").join(dforderdetailsquery.alias("o"),on="productCode",how="inner")
    if __name__ == '__main__':
        df=df.withColumn(row_number().over(Window.orderBy(desc("orderdate"))).alias("rno")).\
            select("p.MSRP", "p.buyPrice", "o.comments", "o.customernumber", "o.orderdate", "o.orderlinenumber",
                     "o.ordernumber", "o.priceeach", "p.productCode", "p.productDescription", "p.productLine",
                     "p.productName", "p.productScale", "p.productVendor", "p.quantityInStock", "o.quantityordered",
                     "o.shippeddate", "o.status")

    return df

def udfProfPromo(cp, sp, qty):
    profit = sp - cp
    profitpercent = (profit / cp) * 100
    if profit > 0:
        profind = 1
    else:
        profind = 0

    if qty > 0 and qty < 1500 and profitpercent > 50.0:
        demandind = 1
    else:
        demandind = 0
    if qty > 1500 and profitpercent > 20.0:
        promoind = 1
    else:
        promoind = 0
    return profit, profitpercent, promoind, demandind

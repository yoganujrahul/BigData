package com.bin;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.spark_project.dmg.pmml.Aggregate.Function;

public class Source {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark=SparkSession.builder().appName("hive project")
				.master("local[*]")
				.enableHiveSupport()
				.getOrCreate();
		
		spark.sparkContext().setLogLevel("WARN");
		StructType schema=new StructType()
				.add("stockName",DataTypes.StringType)
				.add("market",DataTypes.StringType)
				.add("datea",DataTypes.StringType)
				.add("min",DataTypes.FloatType)
				.add("max",DataTypes.FloatType)
				.add("open",DataTypes.FloatType)
				.add("close",DataTypes.FloatType)
				.add("amount",DataTypes.IntegerType)
				.add("adj_close",DataTypes.FloatType);
		StructType schema2=new StructType()
				.add("stockName2",DataTypes.StringType)
				.add("market2",DataTypes.StringType)
				.add("datea2",DataTypes.StringType)
				.add("min2",DataTypes.FloatType)
				.add("max2",DataTypes.FloatType)
				.add("open2",DataTypes.FloatType)
				.add("close2",DataTypes.FloatType)
				.add("amount2",DataTypes.IntegerType)
				.add("adj_close2",DataTypes.FloatType);
		
		Dataset<Row> data=spark.read().option("header","false")
				.schema(schema)
				.csv("data/stocks.csv");
		Dataset<Row> data2=spark.read().option("header","false")
				.schema(schema2)
				.csv("data/stocks.csv");
		//display sample data
		Dataset<Row> op1=data.select("stockName","market","datea","min","max","open",
				"close","amount","adj_close").limit(3);
		op1.show();
		
		//Aggregation 
		data.groupBy("market").count().show();
		//gt- greater than lt- less than, leq-less than equal, geq- greater than equal
		data.select(col("market"),col("min").as("Greater"),col("max")).filter(col("min").gt(30)).limit(3).show();
		data.select(col("market"),col("min").as("LessThan"),col("max")).filter(col("min").lt(40)).limit(3).show();
		data.select(col("market"),col("min").as("LessEqual"),col("max")).filter(col("min").leq(40)).limit(3).show();
		data.select(col("market"),col("min").as("GreatEqual"),col("max")).filter(col("min").geq(40)).limit(3).show();
		
		//sum,min,avg
		data.groupBy("market").agg(functions.sum("min"),functions.avg("max")).orderBy("market").show();
		
		//joins
		Dataset<Row> right=	data.join(data2,data.col("market").equalTo(data2.col("market2")),"right").limit(3);
		right.write().saveAsTable("hivedemo.right");
		
		Dataset<Row> left=data.join(data2,data.col("market").equalTo(data2.col("market2")),"left").limit(3);
		left.write().save("data/left.parquet");
		
		Dataset<Row> full=data.join(data2,data.col("market").equalTo(data2.col("market2")),"full").limit(3);
		full.write().format("jdbc").option("url","jdbc:mysql://localhost:3306/sqoopdemo")
		.option("driver","com.mysql.jdbc.Driver")
		.option("dbtable","full")
		.option("user","root")
		.option("password","123")
		.save();
		
		data.join(data2,data.col("market").equalTo(data2.col("market2")),"inner").limit(3).show();
		
		
			}

}

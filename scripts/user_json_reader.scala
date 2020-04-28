//spark-shell --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" -i user_json_reader.scala

import scala.util.parsing.json._
import spark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,ArrayType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode_outer
import java.io._
import scala.io.Source
import java.nio.charset.StandardCharsets

System.getProperty("file.encoding");

System.setProperty("file.encoding", "UTF-8");

//creating a function for turing a json dataframe into a flat dataframe
def flattenDataframe(df: DataFrame): DataFrame = {
  val fields = df.schema.fields;
  val fieldNames = fields.map(x => x.name);
  val length = fields.length;

  for(i <- 0 to fields.length-1){
    val field = fields(i);
    val fieldtype = field.dataType;
    val fieldName = field.name;
    fieldtype match {
      //exploding arrays into multiple values with names of their fields
      case arrayType: ArrayType =>
        val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName);
        val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName");
        val explodedDf = df.selectExpr(fieldNamesAndExplode:_*);
        return flattenDataframe(explodedDf);
      //renaming any child nodes to new total names
      case structType: StructType =>
        val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname);
        val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames;
        val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))));
       val explodedf = df.select(renamedcols:_*);
        return flattenDataframe(explodedf);
      case _ =>
    }
  }
  df;
}

//creating function to remove date_ from the begining of column names
def removeDataColumnName(df: DataFrame): DataFrame = {
  val fields = df.schema.fields;
  val fieldNames = fields.map(x => x.name);
  val renamedcols = fieldNames.map(x => (col(x.toString()).as(x.toString().replace("data_", ""))));
  val explodedf = df.select(renamedcols:_*);
  return explodedf;
}

//starting file to read from api
var done = false;
var page = 0;

//creating file output encoding to UTF-8
val file = new FileOutputStream("../tmp/temp.json");
val bw = new BufferedWriter(new OutputStreamWriter(file, StandardCharsets.UTF_8));

//write new start of file
bw.write("[");
println("JSON API read ... starting");
while(!done){
  println("page " + page + "...")
  val input = scala.io.Source.fromURL("https://x37sv76kth.execute-api.us-west-1.amazonaws.com/prod/users?page="+ page)("UTF-8");

  val json = input.mkString;

  //getting file info minus the start and end []
  val jsonmin = json.substring(1, json.length()-1);

  //verify if string is empty and set to done if it is
  if(jsonmin != ""){
    //add , between pulls to create new file
    if(page != 0){
      bw.write(",");
    }
    bw.write(jsonmin);
  } else {
    done = true;
  }

  page += 1
}

//write end of json
bw.write("]");
bw.close();

println("JSON API read ... completed");

//load json data
val df_json = spark.read.option("charset", "UTF-8").json("../tmp/temp.json");

//flatten the datastructure out of struts and arrays
val df_flat = flattenDataframe(df_json);

//clean up names removing data column
val df_clean_names = removeDataColumnName(df_flat);

df_clean_names.createOrReplaceTempView("df_clean_names");

spark.sql("drop table if exists users");

println("Writing table users ... starting");
spark.sql("""create table users STORED AS ORC tblproperties ("orc.compress" = "SNAPPY") as select * from df_clean_names""");
println("Writing table users ... completed");

System.exit(0);

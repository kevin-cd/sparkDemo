package com.lyc
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val file=sc.textFile("hdfs://node1:9000/data/README.txt")
    val count=file.flatMap(_.split(" ").map(word=>(word,1))).reduceByKey(_+_)
    count.collect.foreach(println)
  }
}

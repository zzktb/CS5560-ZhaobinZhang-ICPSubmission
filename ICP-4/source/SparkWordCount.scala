

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Mayanka on 09-Sep-15.
 */
object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","D:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")//.set("spark.executor.memory","4g").set("spark.executor.driver","4g")

    val sc=new SparkContext(sparkConf)


    // Inputs: Abstract Folders
    // Outputs: Word count of all the abstracts

    // The folder storing all the input files
    val inputf = sc.wholeTextFiles("inputFolder", 10)

    // Example on how to refer whthin wholeTextFiles
    val input = inputf.map(abs=>{
      //abs._1 // File Path
      abs._2 // File Content
    })

    // inputs
   // val input=sc.textFile("input")

    val wc = input.flatMap(line=>{line.split(" ")}) // return statement I/P RDD[Arrary[String]] O/P RDD[String]
      .map(word=>(word,1)) // I/P RDD[String] O/P RDD{(String, Int)}
      //.cache()

    val output = wc.reduceByKey(_+_)

    output.saveAsTextFile("output")

    val o = output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }

}

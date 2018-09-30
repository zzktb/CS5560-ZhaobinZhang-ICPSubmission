import java.io.File

import NGRAM.getNGrams
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

/**
  * Created by Mayanka on 19-06-2017.
  */
object W2V {
  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "D:\\Mayanka Lenevo F Drive\\winutils")
    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
      .set("spark.driver.memory", "6g").set("spark.executor.memory", "6g")

    val sc = new SparkContext(sparkConf)

    //val input = sc.textFile("data/sample").map(line => line.split(" ").toSeq)
    val input_folder = sc.wholeTextFiles("D:\\umkc\\2018Fall\\Knowledge_discovery\\Tutorial-4-Intro-to-Sparking-Programming\\Spark WordCount\\inputFolder")
    val documents = input_folder.map(abs=>{
      abs._2
    })
    //val input = documents.map(line => line.split(" ").toSeq)
    val input = documents.map(f => {
      val lemmatised = CoreNLP.returnLemma(f)
      val splitString = f.split(" ")
      splitString.toSeq
    })
//    val input = documents.foreach(f=>{
//      val a = getNGrams(f,2)
//    })

    val modelFolder = new File("myModelPath")

    if (modelFolder.exists()) {
      val sameModel = Word2VecModel.load(sc, "myModelPath")
      val synonyms = sameModel.findSynonyms("cancer", 40)

      for ((synonym, cosineSimilarity) <- synonyms) {
        println(s"$synonym $cosineSimilarity")
      }
    }
    else {
      val word2vec = new Word2Vec().setVectorSize(1000)

      val model = word2vec.fit(input)

      val synonyms = model.findSynonyms("cancer", 40)

      for ((synonym, cosineSimilarity) <- synonyms) {
        println(s"$synonym $cosineSimilarity")
      }

      model.getVectors.foreach(f => println(f._1 + ":" + f._2.length))

      // Save and load model
      model.save(sc, "myModelPath")

    }

  }
}

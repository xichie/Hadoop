import scala.collection.mutable._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map

object Knn {

  /*
    * function：样例分类
    * parameter：testSet 待分类样例，trainSet 已知类别的样例，labels trainSet数组对应的类标，K 是近邻数
    * return：样例按照K近邻的类标
    * */
  def classify(testSet: Array[Double], trainSet: Array[Array[Double]], labels: Array[Double], k: Int): Double = {
    val sortedDisIndicies = trainSet.map { x =>
      (testSet,x).zipped.map((ri, si) => math.pow((ri - si), 2)).reduce(_ + _)
    }.zipWithIndex.sortBy(f => f._1).map(f => f._2)

    var classsCount: Map[Char, Int] = Map.empty
    for (i <- 0 to k - 1) {
      val voteIlabel = labels(sortedDisIndicies(i))
      classsCount(voteIlabel.toChar) = classsCount.getOrElse(voteIlabel.toChar, 0) + 1
    }
    classsCount.toArray.sortBy(f => -f._2).head._1

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Knn").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("B:\\gauss1.txt")


    val points = lines.map(line => {
      val array = line.split(" ")
      val attribute = new Array[Double](array.length)
      for (i <- 0 until array.length) {
        attribute(i) = array(i).toDouble
      }
      attribute
    })

    //    样例个数
    var rows = points.count()
    //    样例维度
    var cols = points.first().size - 1
    //    训练数据，不包含类标
    var trainSet = Array.ofDim[Double](rows.toInt / 5 * 4, cols)
    //    训练数据类标，与trainSet的数据一一对应
    var trainSetLabelArray = new Array[Double](rows.toInt / 5 * 4)
    //    测试数据，包含类标
    var testMatrix = Array.ofDim[Double](rows.toInt / 5, cols + 1)
    val testLabelBufferArray = ArrayBuffer[Double]()
    var i = 0
    var j = 0
    val rddList = points.take(rows.toInt)

    /*
    * function：所有数据来源于一个文件，手动设置待分类样例占前20%
    * */
    for (i <- 0 until rows.toInt / 5) {
      for (j <- 0 until cols + 1) {
        testMatrix(i)(j) = rddList(i)(j)
      }
    }

    /*
    * function：所有数据来源于一个文件，手动设置已知样例类别的样例占后80%
    * */
    for (i <- 0 until rows.toInt / 5 * 4) {
      for (j <- 0 until cols) {
        trainSet(i)(j) = rddList(i + rows.toInt / 5)(j)
      }
      trainSetLabelArray(i) = rddList(i + rows.toInt / 5)(cols)
    }

    /*
    * function：将待分类样例的K近邻得到的类标与实际类标打印到控制台上
    * */
    for (i <- 0 until rows.toInt / 5) {
      print("testDataSet：")
      for (j <- 0 until cols) {
        print(testMatrix(i)(j) + " ")
      }
      println("  testDataLabel:" + classify(testMatrix(i), trainSet, trainSetLabelArray, 3) + " realDataLabel：" + testMatrix(i)(cols))
      testLabelBufferArray.append(classify(testMatrix(i), trainSet, trainSetLabelArray, 3))
    }

    for (i <- 0 until testLabelBufferArray.length) {
      println(testLabelBufferArray(i))
    }

    /*
    * function：将最终结果存储在B:\\knnOnSaprkTestLabel路径中
    * */
    var testLabelRDD = sc.parallelize(testLabelBufferArray)
    testLabelRDD.repartition(1).saveAsTextFile("B:\\knnOnSaprkTestLabel")
    sc.stop()
  }
}

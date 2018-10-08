import org.apache.spark.{SparkConf, SparkContext}

object Kmeans extends Exception {

  /*
    * function：找样例所属的簇编号
    * parameter：instance 需判断的样例，centers 簇中心数组
    * return：result 该样例所属的簇中心编号
    * */
  def getNearst(instance: Array[Double], centers: Array[Array[Double]]): Double = {
    var result = -1
    var dist = 1000.0
    var i = 0
    var temp = 0.0
    for (i <- 0 until centers.length) {
      temp = (instance,centers(i)).zipped.map((ri, si) => math.pow((ri - si), 2)).reduce(_ + _)
      if (temp < dist) {
        dist = temp
        result = i
      }
    }
    result
  }

  /*
    * function：两数组对应元素相加合成一个新的数组
    * parameter：a 数组1，b 数组2
    * return：sum 用于两数组对应元素之和
    * */
  def sumArray(a: Array[Double], b: Array[Double]): Array[Double] = {
    var sum = new Array[Double](a.length)
    for (k <- 0 until a.length) {
      sum(k) = a(k) + b(k)
    }
    sum
  }

  /*
    * function：将最后一维的类标改为1，用于属于相同簇中心的样例计数（默认数组的最后一个元素是类别）
    * parameter：a 待改变的数组
    * return：addOne 更改后的数组
    * */
  def addOneArray(a: Array[Double]): Array[Double] = {
    var addOne = new Array[Double](a.length + 1)
    var i = 0
    for (i <- 0 until a.length) {
      addOne(i) = a(i)
    }
    addOne(a.length) = 1
    addOne
  }

  /*
    * function：计算簇中心，数组a前a.length-1个元素是所属同一个簇的样例的向量和（无类别）,最后一个元素是所属同一个簇的样例的个数
    * parameter：a 属于相同簇的所有样例对应分量相加之和，其中最后一个分量用于标记属于同一个簇的样例个数
    * return：resultArray 更新后的簇中心
    * */
  def centersCalculate(a: Array[Double]): Array[Double] = {
    var resultArray = new Array[Double](a.length - 1)
    var i = 0
    for (i <- 0 until a.length - 1) {
      resultArray(i) = a(i) / a(a.length - 1)
    }
    resultArray
  }

  /*
    * function：随机生成k个簇中心的标号数组
    * parameter：k 簇中心个数，n 样例个数
    * return：resultList 簇中心标号数组
    * */
  def randomCentersIndex(k: Int, n: Int) = {
    var resultList: List[Int] = Nil
    while (resultList.length < k) {
      val randomNum = (new util.Random).nextInt(n)
      if (!resultList.exists(s => s == randomNum)) {
        resultList = resultList ::: List(randomNum)
      }
    }
    resultList
  }

  /*
    * function：根据簇中心标号数组，生成初始簇中心
    * parameter：k 簇中心个数，n 样例个数，listArray 簇中心标号数组
    * return：centers 初始簇中心
    * */
  def generateCenters(k: Int, n: Int,listArray:Array[Array[Double]]):Array[Array[Double]]={
    var centersIndexArray = randomCentersIndex(k, listArray.length)
    var centers = Array.ofDim[Double](k, listArray(0).length-1)
    for (i <- 0 until k) {
      for (j <- 0 until listArray(0).length-1) {
        centers(i)(j) = listArray(centersIndexArray(i))(j)
      }
    }
    centers
  }

  /*
    * function：将更新完毕后的簇中心保存成一维数组
    * parameter：centers 簇中心样例，realCentersLength 簇中心个数
    * return：saveFileArray 保存簇中心的数组
    * */
  def saveFinalCentersToFile(centers:Array[Array[Double]],realCentersLength:Int):Array[Double]={
     var saveFileArray = Array.ofDim[Double](centers(0).length * realCentersLength)
     var p = 0
     for (p <- 0 until realCentersLength) {
       for (q <- 0 until centers(0).length) {
         saveFileArray(p * centers(0).length + q) = centers(p)(q)
       }
     }
    saveFileArray
  }

  /*
    * function：判断簇中心的真正个数
    * parameter：realCentersLength 簇中心更新过程中个数，k 初始簇中心个数
    * return：centersLength 当前簇中心个数
    * */
  def identifyRealCentersLength(realCentersLength:Int,k:Int):Int={
    var centersLength = 0
    if (realCentersLength == 0) (
      centersLength = k
      ) else {
      centersLength = realCentersLength
    }
      centersLength
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Kmeans").setMaster("local")
    val sc = new SparkContext(conf)
    //    以高斯数据为例，数据有类标
    val lines = sc.textFile("B:\\gauss3.txt")

    /*
    * function：获取文件内容保存在dataRDD中
    * */
    val dataRDD = lines.map(line => {
      val array = line.split(" ")
      val attribute = new Array[Double](array.length)
      for (i <- 0 until array.length) {
        attribute(i) = array(i).toDouble
      }
      attribute
    })
    //    样例个数
    val rows = dataRDD.count()
    //    样例属性个数,-1是因为最后一维是所属类别
    val cols = dataRDD.first().size - 1

    var i = 0
    var j = 0
    //    簇中心个数
    var k = 4

    /*
    * function：生成初始簇中心保存在centers中
    * */
    val rddList = dataRDD.take(rows.toInt)
    var centers = Array.ofDim[Double](k, cols)
    centers=generateCenters(k,rows.toInt,rddList)

    var count = 0
    //    实际簇中心个数
    var realCentersLength = 0
    //    以簇中心更新50次为例
    for (count <- 0 until 50) {

      //    判断簇中心的真正个数：centersLength
      var centersLength = identifyRealCentersLength(realCentersLength,k)

      var m = 0
      var n = 0
      println("第"+count+"次簇中心是   ")
      for (m <- 0 until (centersLength)) {
        for (n <- 0 until centers(0).length) {
          print(centers(m)(n) + "  ")
        }
        println()
      }

      var centersRDD = sc.parallelize(centers)

      val sumRDD = dataRDD.map({ f =>
        val arrayF = new Array[Double](f.length - 1)
        var i = 0
        for (i <- 0 until f.length - 1) {
          arrayF(i) = f(i)
        }
        (getNearst(arrayF, centers), arrayF)
      }).map(x => (x._1, addOneArray(x._2))).reduceByKey((x, y) => sumArray(x, y))

      //    簇中心实际长度
      realCentersLength = sumRDD.collect().length

      //    centers数组行数改变，簇中心减少
      if (k != realCentersLength) {
        centers = Array.ofDim[Double](realCentersLength, cols)
      }

      /*
       * function：更新后的簇中心保存在centers中
       * */
      centersRDD = sumRDD.map(x => (centersCalculate(x._2)))
      var centersRDDArray = centersRDD.map(x => x).collect()
      for (i <- 0 until centersRDDArray.length) {
        for (j <- 0 until centersRDDArray(0).length) {
          centers(i)(j) = centersRDDArray(i)(j)
        }
      }
    } //    簇中心更新完毕

    /*
    * function：将最终结果存储在B:\kmeansOnSparkTestLabel路径中
    * */
    var saveFileArray = Array.ofDim[Double](centers(0).length * realCentersLength)
    saveFileArray=saveFinalCentersToFile(centers,realCentersLength)
    var saveFileRDD = sc.parallelize(saveFileArray.toList)
    saveFileRDD.repartition(1).saveAsTextFile("B:\\kmeansOnSparkTestLabel")

    sc.stop()
  }
}
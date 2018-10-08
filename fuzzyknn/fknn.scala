import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object fknn {
  def main(args: Array[String]): Unit = {
    //    System.setProperty("hadoop.home.dir","C:\\Users\\WANG\\Desktop\\hadoop-2.7.3")

    /*
    * function：计算欧式距离
    * parameter：a 数组1，b 数组2
    * return：数组a和数组b之间的欧式距离
    * */
    def Distance(a: Array[Double], b: Array[Double]): Double = {
      if (a.length != b.length) {
        throw new Exception("Dimensions do not match")
      }
      else {
        math.sqrt((a, b).zipped.map((ri, si) => math.pow((ri - si), 2)).reduce(_ + _))
      }
    }

    /*
    * function：两数组对应元素相加合成一个新的数组
    * parameter：a 数组1，b 数组2
    * return：sum 用于两数组对应元素之和
    * */
    def addArray(a: Array[Double], b: Array[Double]): Array[Double] = {
      val sum = new Array[Double](a.length)
      for (k <- 0 until a.length) {
        sum(k) = a(k) + b(k)
      }
      sum
    }

    /*
    * function：计算测试集的隶属度
    * parameter：a 操作数据格式：（测试样例，（训练样例，与训练样例的距离，训练样例对应的隶属度））
    * return：resultarr 隶属度
    * */
    def memShipDevide(a: Array[(Array[Double], Double, Array[Double])]): ArrayBuffer[Double] = {
      //    k为近邻数
      val k = 3
      //    m为隶属度维度
      val m = 2
      //    构造分子变长数组
      val numeratorarr = ArrayBuffer[Double]()
      //    构造分母变长数组
      val denominatorsumarr = ArrayBuffer[Double]()
      //    构造隶属度变长数组
      val resultarr = ArrayBuffer[Double]()
      //    初始化分子变长数组
      for (j <- 0 until m) {
        var sum = 0.0
        for (i <- 0 until k) {
          //    会导致nan，加1
          val result = (1 + 1) / (a(i)._2 + 1) * a(i)._3(j)
          sum += result
        }
        numeratorarr += sum
      }
      //   初始化分母变长数组
      for (j <- 0 until m) {
        var sum = 0.0
        for (i <- 0 until k) {
          //   会导致nan，加1
          sum += (1 + 1) / (a(i)._2 + 1)
        }
        denominatorsumarr += sum
      }
      //  初始化隶属度数组
      for (i <- 0 until m) {
        var result = numeratorarr(i) / denominatorsumarr(i)
        resultarr += result
      }
      resultarr
    }

    val conf = new SparkConf().setAppName("Fuzzy Knn").setMaster("local")
    val sc = new SparkContext(conf)
    //    读取训练集
    val trainData = sc.textFile("B:\\trainset.txt")
    //    读取测试集
    val testData = sc.textFile("B:\\testset.txt")
    //    初始化测试集，测试集无类表，只有属性值
    val testInitRDD = testData.map(line => {
      val array = line.split(" ")
      val attribute = new Array[Double](array.length)
      for (i <- 0 until array.length) {
        attribute(i) = array(i).toDouble
      }
      attribute
    })

    /*
    * function:初始化训练数据集
    * result:（类标，属性值）
    * */
    val trainInitRDD = trainData.map(line => {
      val arr = line.split(" ")
      val attribute = new Array[Double](arr.length - 1)
      for (i <- 0 until arr.length - 1) {
        attribute(i) = arr(i).toDouble
      }
      val label = arr(arr.length - 1).toString
      (label, attribute)
    })
    //    trainInitRDD.foreach(print)

    /*
    * funtion:计算trainSet每类中的样例个数,默认为true，即升序
    * result:升序排列每类样例个数
    * */
    val inSumRDD = trainInitRDD.map(line => {
      (line._1, 1)
    }).reduceByKey(_ + _).sortByKey(true).map(_._2)
    //    inSumRDD.foreach(println)

    /*
    * function:计算每个类的各属性和
    * result:List(每类的属性值和)
    * */
    val sumRDD = trainInitRDD.reduceByKey((x, y) => addArray(x.toArray, y.toArray)).map(k => {
      (k._1, k._2)
    }).sortByKey(true).map(_._2)
    //    sumRDD.foreach(x=>println((x.toList)))

    /*
     *  function：计算类中心
     *  result:（类别个数，对应类别中心)
     */
    val centerRDD = inSumRDD.zip(sumRDD).map(k => {
      for (i <- 0 until k._2.length) {
        k._2(i) = k._2(i) / k._1
      }
      (k._1, k._2)
    })
    //    centerRDD.foreach(x=>println(x._1,(x._2.toList)))

    /*
    * function:计算每个样例到各中心的距离的平方的倒数
    * result:(类标，List(样例)，该样例到第i类中心的距离的平方的倒数)
    * */
    val instanceDisRDD = trainInitRDD.cartesian(centerRDD.map(_._2)).map(line => {
      var dis = Distance(line._1._2, line._2)
      var result = 0.0
      if (dis == 0) {
        result = Double.MaxValue
      }
      else {
        result = math.pow(dis, -2)
      }
      (line._1, result)
    })
    //    instanceDisRDD.foreach(x => println((x._1._1, x._1._2.toList, x._2)))

    /*
    * function:计算每个样例到各类中心的距离的平方的倒数的和
    * result:(类标，List(样例)，该样例到第i类中心的距离的平方的倒数的和)
    * */
    val insCenterDisRDD = instanceDisRDD.map(line => {
      ((line._1._1, line._1._2), line._2)
    }).reduceByKey(_ + _)
    //        insCenterDisRDD.foreach(x =>println((x._1._1,x._1._2.toList,x._2)))

    /*
    * function:计算隶属度矩阵
    * result:(List(样例)，List(存放该样例对每类中心的隶属度))
    * */
    val memshipMatRDD = instanceDisRDD.cartesian(insCenterDisRDD).filter(line => {
      line._1._1._2.toList == line._2._1._2.toList
    }).map(line => {
      (line._1, line._2._2)
    }).map(line => {
      val result = (line._1._2) / (line._2)
      (line._1._1._2.toList, result)
    }).groupByKey()
    //    memshipMatRDD.foreach(x=>println((x._1,x._2.toList)))

    /*
    * function：信息组合
    * result：（测试样例，（训练样例，与训练样例的距离，训练样例对应的隶属度））
    * */
    val testMemShipNeedInfoRDD = testInitRDD.cartesian(memshipMatRDD).map(line => {
      val dis = Distance(line._1, line._2._1.toArray)
      (line._1.toList, (line._2._1.toArray, dis, line._2._2.toArray))
    }).sortBy(_._2._2).groupByKey()
    //    testMemShipNeedInfoRDD.foreach(x=>println(x._1,x._2.toList))

    /*
    * function：计算测试样例的隶属度
    * result：（List(测试样例)，ArrayBuffer（该测试样例对应的隶属度））
    * */
    val testMemShipMat = testMemShipNeedInfoRDD.map(line => {
      val result = memShipDevide(line._2.toArray)
      (line._1, result)
    })
    //    testMemShipMat.foreach(println)
    //    将结果输出到HDFS
    testMemShipMat.coalesce(1,false).saveAsTextFile(args(2))
    sc.stop()
  }
}



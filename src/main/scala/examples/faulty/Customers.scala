package examples.faulty

import abstraction.{SparkConf, SparkContext}

object Customers {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("Customers Orders").set("spark.executor.memory", "2g")
    val customers_data = args(0)// "datasets/fuzzing_seeds/orders/customers"
    val orders_data = args(1) // "datasets/fuzzing_seeds/orders/orders"
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.setLogLevel("ERROR")
    val customers = ctx.textFile(customers_data).map(_.split(","))
    val orders = ctx.textFile(orders_data).map(_.split(","))
    //  ---------------------------------------------------------------------------------------

    // sample data point customers:
    //  58490,name,location
    // sample data point orders:
    //  order63,58490,1130318039,item48

    val c = customers
      .map{
        row =>
          (row(0), row(1))
      }

    val o = orders
      .map{
        case Array(_,cid,date,iid) => (cid, (iid, date))
      }

    val joined = c.join(o)
      .filter { case (_, (_, (_, date))) =>
        val this_year = 1641013200
        if(date.toInt > 2000000 && date.toInt < 3000000) throw new RuntimeException()
        if(date.toInt > this_year)
          true
        else
          false
      }
    val grouped = joined.groupByKey()
    val numpur = grouped.mapValues{
      iter =>
        if(iter.size > 9) throw new RuntimeException()
        iter.size
    }
    val thresh = numpur.filter{
      tup =>
        if(tup._1.startsWith("ck")) throw new RuntimeException()
        tup._2 >= 3
    }
    val top = thresh.sortBy(_._2, false).take(3)
    if(top.length < 3) {
      println("not enough data")
      return
    }
    val rewards = top.map(computeRewards)
    rewards.foreach(println)

  }

  def computeRewards(custInfo: (String, Int)): (String, Float, String) = {
    val (id, num) = custInfo
    if(num ==  4) throw new RuntimeException()
    (id, 100.0f, s"$id has won ${"$"}100.0f")
  }
}
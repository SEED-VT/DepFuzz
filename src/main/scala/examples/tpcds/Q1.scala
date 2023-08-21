package examples.tpcds

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object Q1 extends Serializable {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("TPC-DS Query 1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val datasetsPath = "./data_tpcds"
    val seed = "ahmad".hashCode()
    val rand = new Random(seed)
    val YEAR = rand.nextInt(2002 - 1998) + 1998
    val STATE = "TN"

    val store_returns = sc.textFile(s"$datasetsPath/store_returns").map(_.split(","))
    val date_dim = sc.textFile(s"$datasetsPath/date_dim").map(_.split(","))
    val store = sc.textFile(s"$datasetsPath/store").map(_.split(","))
    val customer = sc.textFile(s"$datasetsPath/customer").map(_.split(","))

    val customers_total_return = store_returns
      .map(row => (row.last, row))
      //select _ from store_returns ,date_dim
      .join(date_dim
        .filter(row => row(6) == YEAR.toString)
        .map(row => (row.head, row))
      )
      .map { row =>
        val store_returns_row = row._2._1
        val sr_customer_sk = store_returns_row(2)
        val sr_store_sk = store_returns_row(6)
        // sum([AGG_FIELD])
        val sum_agg_field = List(10, 11, 12, 13, 15, 16, 17).map(n => convertColToFloat(store_returns_row, n)).sum
        ((sr_customer_sk, sr_store_sk), (sr_customer_sk, sr_store_sk, sum_agg_field))
      }
      // group by sr_customer_sk ,sr_store_sk
      .reduceByKey {
        case ((r1c1, r1c2, sum1), (r2c1, r2c2, sum2)) =>
          (r1c1, r2c2, sum1 + sum2)
      }
    // ----This following section of code implements this section of the original SQL query--------------
    //      select avg (ctr_total_return) * 1.2
    //      from customer_total_return ctr2
    //      where ctr1.ctr_store_sk = ctr2.ctr_store_sk
    val final_table = customers_total_return.map {
      case ((sr_customer_sk, sr_store_sk), rest) => (sr_store_sk, rest)
    }
      .join(store.map(row => (row.head, row)))
      .map{
        case (store_sk, (ctr_row @ (sr_customer_sk, st_store_sk, sum_agg_field), store_row)) => (sr_customer_sk, (ctr_row, store_row))
      }
      .join(customer.map(row => (row.head, row)))
      .map{
        case (customer_sk, ((ctr_row, store_row), customer_row)) => (ctr_row, store_row, customer_row)
      }


    val red = final_table.map {
      case ((_,_,total_return), _, _) => (total_return, 1)
    }.reduce{case ((v1, c1), (v2, c2)) => (v1+v2, c1+c2)}

    val avg = red._1 / red._2 * 1.2

    // ---------------------------------------------------------------------------------------
    final_table
      .filter {
        case ((_, _, return_total), store_row, _) =>
          val cond = store_row(24) == STATE.toString
//          println(cond, s"${store_row(24)} == ${STATE}")
          return_total > avg && cond
      }
      .map {
        case (_, _, customer_row) => customer_row(1)
      }
      .take(5).foreach(println)

  }

  def convertColToFloat(row: Array[String], col: Int): Float = {
    try {
      row(col).toFloat
    } catch {
      case _ => 0
    }
  }
  /* ORIGINAL QUERY:
  define COUNTY = random(1, rowcount("active_counties", "store"), uniform);
  define STATE = distmember(fips_county, [COUNTY], 3);
  define YEAR = random(1998, 2002, uniform);
  define AGG_FIELD = text({"SR_RETURN_AMT",1},{"SR_FEE",1},{"SR_REFUNDED_CASH",1},{"SR_RETURN_AMT_INC_TAX",1},{"SR_REVERSED_CHARGE",1},{"SR_STORE_CREDIT",1},{"SR_RETURN_TAX",1});
  define _LIMIT=100;

  with customer_total_return as
  (
      select sr_customer_sk as ctr_customer_sk ,sr_store_sk as ctr_store_sk ,sum([AGG_FIELD])
                                                                                  as ctr_total_return
      from store_returns ,date_dim
      where sr_returned_date_sk = d_date_sk
      and d_year =[YEAR]
      group by sr_customer_sk ,sr_store_sk
  )
  [_LIMITA]

  select [_LIMITB] c_customer_id
  from customer_total_return ctr1 ,store ,customer
  where ctr1.ctr_total_return >   (
                                      -- subquery 1
                                      select avg(ctr_total_return)*1.2
                                      from customer_total_return ctr2
                                      where ctr1.ctr_store_sk = ctr2.ctr_store_sk
                                  )
  and s_store_sk = ctr1.ctr_store_sk
  and s_state = '[STATE]'
  and ctr1.ctr_customer_sk = c_customer_sk
  order by c_customer_id
  [_LIMITC];
   */
}
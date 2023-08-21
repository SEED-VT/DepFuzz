package examples.tpcds

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Q3 extends Serializable {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("TPC-DS Query 3")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val datasetsPath = "./data_tpcds"
    val seed = "ahmad".hashCode()
    val rand = new Random(seed)
    val MANUFACT = rand.nextInt(1000 - 1) + 1
    val MONTH = rand.nextInt(2)+11

    val store_sales = sc.textFile(s"$datasetsPath/store_sales").map(_.split(","))
    val date_dim = sc.textFile(s"$datasetsPath/date_dim").map(_.split(","))
    val item = sc.textFile(s"$datasetsPath/item").map(_.split(","))

    val check = date_dim
      .filter(row => row(8)/*d_moy*/ == MONTH.toString)
      .map(row => (row.head, row))
      // t.d_date_sk = store_sales.ss_sold_date_sk
      .join(store_sales.map(row => (row.last, row)))
      .map{
        case (date_sk, (date_dim_row, ss_row)) =>
          (ss_row(1)/*ss_item_sk*/, (date_dim_row, ss_row))
      }
      // and store_sales.ss_item_sk = item.i_item_sk
      .join {
        item
          .filter(row => row(13) /*i_manufact_id*/ == MANUFACT.toString) // and item.i_manufact_id = [MANUFACT]
          .map(row => (row.head, row))
      }
      .map {
        case (item_sk, ((date_dim_row, ss_row), item_row)) =>
          val ss_ext_sales_price = convertColToFloat(ss_row, 14)
          val ss_sales_price = convertColToFloat(ss_row, 12)
          val ss_ext_discount_amt = convertColToFloat(ss_row, 13)
          val ss_net_profit = convertColToFloat(ss_row, 21)
          val sum = ss_net_profit + ss_sales_price + ss_ext_sales_price + ss_ext_discount_amt

          val d_year = date_dim_row(6)
          val i_brand = item_row(8)
          val i_brand_id = item_row(7)

          ((d_year, i_brand, i_brand_id), (d_year, i_brand, i_brand_id, sum))
      }
      .reduceByKey {
        case ((d_year, i_brand, i_brand_id, v1),(_, _, _, v2)) =>
          (d_year, i_brand, i_brand_id, v1+v2)
      }
      .map {
        case (_, (d_year, i_brand, i_brand_id, agg)) => (d_year, i_brand_id, i_brand, agg)
      }
      .take(10).foreach(println)



    /*
    define AGGC= text({"ss_ext_sales_price",1},{"ss_sales_price",1},{"ss_ext_discount_amt",1},{"ss_net_profit",1});
    define MONTH = random(11,12,uniform);
    define MANUFACT= random(1,1000,uniform);
    define _LIMIT=100;

    [_LIMITA] select [_LIMITB] dt.d_year
          ,item.i_brand_id brand_id
          ,item.i_brand brand
          ,sum([AGGC]) sum_agg
    from  date_dim dt
         ,store_sales
         ,item
    where dt.d_date_sk = store_sales.ss_sold_date_sk
      and store_sales.ss_item_sk = item.i_item_sk
      and item.i_manufact_id = [MANUFACT]
      and dt.d_moy=[MONTH]
    group by dt.d_year
         ,item.i_brand
         ,item.i_brand_id
    order by dt.d_year
            ,sum_agg desc
            ,brand_id
    [_LIMITC];

    */

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
package examples.tpcds

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Q19 extends Serializable {

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
    val MONTH = rand.nextInt(2)+11
    val MANAGER = "50"

    val date_dim = sc.textFile(s"$datasetsPath/date_dim").map(_.split(","))
    val store_sales = sc.textFile(s"$datasetsPath/store_sales").map(_.split(","))
    val item = sc.textFile(s"$datasetsPath/item").map(_.split(","))
    val customer = sc.textFile(s"$datasetsPath/customer").map(_.split(","))
    val customer_address = sc.textFile(s"$datasetsPath/customer_address").map(_.split(","))
    val store = sc.textFile(s"$datasetsPath/store").map(_.split(","))

    val filtered_i = item
      .filter {
        row =>
          val i_manager_id = row(row.length-2)
          i_manager_id == MANAGER
      }

    val filtered_dd = date_dim
      .filter {
        row =>
          val d_moy = row(8)
          val d_year = row(6)
          d_moy == MONTH.toString && d_year == YEAR.toString
      }
    date_dim
      .map(row => (row.head, row))
      .join(store_sales.map(row => (row.last /*ss_sold_date_sk*/, row)))
      .map {
        case (_, (dd_row, ss_row)) =>
          (ss_row(1) /*ss_item_sk*/, (dd_row, ss_row))
      }
      .join(item.map(row => (row.head, row)))
      .map {
        case (_, ((dd_row, ss_row), i_row)) =>
          (ss_row(2)/*ss_customer_sk*/, (dd_row, ss_row, i_row))
      }
      .join(customer.map(row => (row.head, row)))
      .map {
        case (_, ((dd_row, ss_row, i_row), c_row)) =>
          (c_row(4)/*c_current_addr_sk*/, (dd_row, ss_row, i_row, c_row))
      }
      .join(customer_address.map(row => (row.head, row)))
      .map {
        case (_, ((dd_row, ss_row, i_row, c_row), ca_row)) =>
          (ss_row(6)/*ss_store_sk*/, (dd_row, ss_row, i_row, c_row, ca_row))
      }
      .join(store.map(row => (row.head, row)))
      .map {
        case (_, ((dd_row, ss_row, i_row, c_row, ca_row), s_row)) =>
          (dd_row, ss_row, i_row, c_row, ca_row, s_row)
      }
      .filter {
        case (_, _, _, _, ca_row, s_row) =>
          val ca_zip = try { ca_row(9) } catch { case _ => "error"}
          val s_zip = s_row(25)
          ca_zip.take(5) != s_zip.take(5)
      }
      .map {
        case (_, ss_row, i_row, _, _, _) =>
          val ss_ext_sales_price = convertColToFloat(ss_row, 14)
          val i_brand_id = i_row(7)
          val i_brand = i_row(8)
          val i_manufact_id = i_row(13)
          val i_manufact = i_row(14)
          ((i_brand_id, i_brand, i_manufact_id, i_manufact), ss_ext_sales_price)
      }
      .reduceByKey(_+_)
      .sortBy(_._1)
      .take(10)
      .foreach(println)
    
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
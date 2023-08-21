package examples.tpcds

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Q15 extends Serializable {

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
    val QOY = rand.nextInt(1) + 1
    val ZIPS = List("85669","86197","88274","83405","86475","85392","85460","80348","81792")
    val STATES = List("CA", "WA", "GA")

    val catalog_sales = sc.textFile(s"$datasetsPath/catalog_sales").map(_.split(","))
    val customer = sc.textFile(s"$datasetsPath/customer").map(_.split(","))
    val customer_address = sc.textFile(s"$datasetsPath/customer_address").map(_.split(","))
    val date_dim = sc.textFile(s"$datasetsPath/date_dim").map(_.split(","))

    val filtered_dd = date_dim
      .filter {
        row =>
          val d_qoy = row(10)
          val d_year = row(6)
          d_qoy == QOY.toString && d_year == YEAR.toString
      }

    catalog_sales
      .map(row => (row(2)/*cs_bill_customer_sk*/, row))
      .join(customer.map(row => (row.head, row)))
      .map {
        case (_, (cs_row, c_row)) =>
          (c_row(4)/*c_current_addr_sk*/, (cs_row, c_row))
      }
      .join(customer_address.map(row => (row.head, row)))
      .map {
        case (_, ((cs_row, c_row), ca_row)) =>
          (cs_row.last/*cs_sold_date_sk*/, (cs_row, c_row, ca_row))
      }
      .filter {
        case (_, (cs_row, c_row, ca_row)) =>
          val ca_zip = try { ca_row(9) } catch { case _ => "error"} // took liberty here (if the row is malformed for some reason
          val ca_state = try { ca_row(8) } catch { case _ => "error"}
          val cs_sales_price = convertColToFloat(cs_row, 20)

          ca_zip != "error" && ca_state != "error" &&
            (ZIPS.contains(ca_zip.take(5)) || cs_sales_price > 500 || STATES.contains(ca_state))
      }
      .join(filtered_dd.map(row => (row.head, row)))
      .map {
        case (_, ((cs_row, c_row, ca_row), dd_row)) =>
          val cs_sales_price = convertColToFloat(cs_row, 20)
          (ca_row(9)/*ca_zip*/, cs_sales_price)
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
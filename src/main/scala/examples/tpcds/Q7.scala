package examples.tpcds

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Q7 extends Serializable {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("TPC-DS Query 1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val datasetsPath = "./data_tpcds"
    val seed = "ahmad".hashCode()
    val rand = new Random(seed)
    val YEAR = rand.nextInt(2003 - 1998) + 1998
    val GENDER = "M"
    val MS = "M" // marital status
    val ES = "Primary" // education status

    val customer_demographics = sc.textFile(s"$datasetsPath/customer_demographics").map(_.split(","))
    val promotion = sc.textFile(s"$datasetsPath/promotion").map(_.split(","))
    val store_sales = sc.textFile(s"$datasetsPath/store_sales").map(_.split(","))
    val date_dim = sc.textFile(s"$datasetsPath/date_dim").map(_.split(","))
    val item = sc.textFile(s"$datasetsPath/item").map(_.split(","))


    val filter_cd = customer_demographics.filter {
      row =>
        val cd_gender = row(1)
        val cd_marital_status = row(2)
        val cd_education_status = row(3)

        cd_gender == GENDER && cd_marital_status == MS && cd_education_status == ES
    }
    val filtered_p = promotion.filter {
      row =>
        val p_channel_email = row(9)
        val p_channel_event = row(14)
        p_channel_email == "N" && p_channel_event == "N"
    }

    val filtered_dd = date_dim.filter {
      row =>
        val d_year = row(6)
        d_year == YEAR.toString
    }

    store_sales
      .map(row => (row.last /*ss_sold_date_sk*/, row))
      .join(filtered_dd.map(row => (row.head/*d_date_sk*/, row)))
      .map {
        case (date_sk, (ss_row, dd_row)) =>
          (ss_row(1)/*ss_item_sk*/, (ss_row, dd_row))
      }
      .join(item.map(row => (row.head, row)))
      .map {
        case (item_sk, ((ss_row, dd_row), i_row)) =>
          (ss_row(3)/*ss_cdemo_sk*/, (ss_row, dd_row, i_row))
      }
      .join(filter_cd.map(row => (row.head, row)))
      .map {
        case (cdemo_sk, ((ss_row, dd_row, i_row), cd_row)) =>
          (ss_row(7)/*ss_promo_sk*/, (ss_row, dd_row, i_row, cd_row))
      }
      .join(filtered_p.map(row => (row.head, row)))
      .map {
        case (promo_sk, ((ss_row, dd_row, i_row, cd_row), p_row)) =>
          val ss_quantity = convertColToFloat(ss_row, 9)
          val ss_list_price = convertColToFloat(ss_row, 11)
          val ss_coupon_amt = convertColToFloat(ss_row, 18)
          val ss_sales_price = convertColToFloat(ss_row, 12)

          (i_row(1)/*i_item_id*/, (ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price, 1))
      }
      .reduceByKey {
        case ((a1, a2, a3, a4, count1), (b1, b2, b3, b4, count2)) =>
          (a1+b1, a2+b2, a3+b3, a4+b4, count1+count2)
      }
      .map {
        case (i_item_id, (sum1, sum2, sum3, sum4, count)) =>
          (i_item_id, sum1/count, sum2/count, sum3/count, sum4/count)
      }
      .sortBy(_._1)
      .take(10)
      .foreach(println)

    /*
define GEN= dist(gender, 1, 1);
    define MS= dist(marital_status, 1, 1);
    define ES= dist(education, 1, 1);
    define YEAR = random(1998,2002,uniform);
    define _LIMIT=100;

    [_LIMITA] select [_LIMITB] i_item_id,
           avg(ss_quantity) agg1,
           avg(ss_list_price) agg2,
           avg(ss_coupon_amt) agg3,
           avg(ss_sales_price) agg4
    from store_sales, customer_demographics, date_dim, item, promotion
    where ss_sold_date_sk = d_date_sk and
          ss_item_sk = i_item_sk and
          ss_cdemo_sk = cd_demo_sk and
          ss_promo_sk = p_promo_sk and
          cd_gender = '[GEN]' and
          cd_marital_status = '[MS]' and
          cd_education_status = '[ES]' and
          (p_channel_email = 'N' or p_channel_event = 'N') and
          d_year = [YEAR]
    group by i_item_id
    order by i_item_id
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
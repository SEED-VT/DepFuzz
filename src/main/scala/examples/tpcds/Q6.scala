package examples.tpcds

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Q6 extends Serializable {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("TPC-DS Query 1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val datasetsPath = "./data_tpcds"
    val seed = "ahmad".hashCode()
    val rand = new Random(seed)
    val MONTH = rand.nextInt(12)+1
    val YEAR = rand.nextInt(2002 - 1998) + 1998

    val customer_address = sc.textFile(s"$datasetsPath/customer_address").map(_.split(","))
    val customer = sc.textFile(s"$datasetsPath/customer").map(_.split(","))
    val store_sales = sc.textFile(s"$datasetsPath/store_sales").map(_.split(","))
    val date_dim = sc.textFile(s"$datasetsPath/date_dim").map(_.split(","))
    val item = sc.textFile(s"$datasetsPath/item").map(_.split(","))



    val subquery1_result = date_dim.filter { row =>
      val d_year = row(6)
      val d_moy = row(8)
      d_year == YEAR.toString && d_moy == MONTH.toString
    }
      .map(row => row(3) /*d_month_seq*/)
      .distinct
      .take(1)
      .head


    val main_query_part1 = customer_address
      .map(row => (row.head, row))
      .join(customer.map(row => (row(4)/*c_current_addr_sk*/, row)))
      .map {
        case (addr_sk, (ca_row, c_row)) =>
          (c_row.head /*c_customer_sk*/, (ca_row, c_row))
      }
      .join(store_sales.map(row => (row(2)/*ss_customer_sk*/, row)))
      .map {
        case (customer_sk, ((ca_row, c_row), ss_row)) =>
          (ss_row.last/*ss_sold_date_sk*/, (ca_row, c_row, ss_row))
      }
      .join(date_dim.map(row => (row.head /*d_date_sk*/, row)))
      .map {
        case (date_sk, ((ca_row, c_row, ss_row), dd_row)) =>
          (ss_row(1)/*ss_item_sk*/, (ca_row, c_row, ss_row, dd_row))
      }
      .join(item.map(row => (row.head/*i_item_sk*/, row)))
      .map {
        case (item_sk, ((ca_row, c_row, ss_row, dd_row), i_row)) =>
          (ca_row, c_row, ss_row, dd_row, i_row)
      }

    // Not sure if this should be applied to the original item table or the partial result
    // confirm with someone
    val reduce1 = main_query_part1
      .map {
        case (_, _, _, _, i_row) =>
          (convertColToFloat(i_row, 5), 1) // j.i_current_price in sql query
      }
      .reduce {
        case ((v1, c1), (v2, c2)) =>
          (v1 + v2, c1 + c2)
      }

    val subquery2_result = reduce1._1/reduce1._2


    main_query_part1
      .filter {
        case (_, _, _, dd_row, _) =>
          dd_row(3)/*d_month_seq*/ == subquery1_result
      }
      .filter {
        case (_, _, _, _, i_row) =>
          val i_current_price = convertColToFloat(i_row, 5)
          i_current_price > 1.2 * subquery2_result
      }
      .map {
        case (ca_row, c_row, ss_row, dd_row, i_row) =>
//          println(ca_row.mkString(", "))
          (try { ca_row(8) /*ca_state*/ } catch { case _ => "NULL"}, 1) // Took some liberty here, ca_row(8) fails due to array out of bounds
      }
      .reduceByKey(_+_)
      .filter {
        case (state, count) =>
          count > 10
      }
      .sortBy(_._2)
      .take(10)
      .sortWith {case (a, b) => (a._2 < b._2) || (a._2 == b._2 && a._1 < b._1)}
      .foreach{
        case (state, count) => println(state, count)
      }






    /*
    define YEAR = random(1998, 2002, uniform);
    define MONTH= random(1,7,uniform);
    define _LIMIT=100;

    [_LIMITA] select [_LIMITB] a.ca_state state, count(*) cnt
    from customer_address a
        ,customer c
        ,store_sales s
        ,date_dim d
        ,item i
    where       a.ca_address_sk = c.c_current_addr_sk
      and c.c_customer_sk = s.ss_customer_sk
      and s.ss_sold_date_sk = d.d_date_sk
      and s.ss_item_sk = i.i_item_sk
      and d.d_month_seq =
           (select distinct (d_month_seq)
            from date_dim
                  where d_year = [YEAR]
              and d_moy = [MONTH] )
      and i.i_current_price > 1.2 *
                (select avg(j.i_current_price)
           from item j
           where j.i_category = i.i_category)
    group by a.ca_state
    having count(*) >= 10
    order by cnt, a.ca_state
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
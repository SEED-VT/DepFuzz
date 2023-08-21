package schemas

import fuzzer.{Guidance, Schema}
import scoverage.Coverage
import utils.FileUtils


object BenchmarkSchemas {
  val SYNTHETIC1 = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    ),
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val SYNTHETIC2 = Array[Array[Schema[Any]]](
    // Dataset 1: Alfreds,Maria,Germany,1,1
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    ),
    // Dataset 2: 10308,2,1996-09-18
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val SYNTHETIC3 = Array[Array[Schema[Any]]](
    // Dataset 1: 1185,PG0134,"2017-09-10 02:50:00.000","2017-09-10 07:55:00.000",DME,BTK,Scheduled,319,,
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER)
    ),
    // Dataset 2: YKS,"Yakutsk Airport",Yakutsk,129.77099609375,62.0932998657227,Asia/Yakutsk
    Array(
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_CATEGORICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_CATEGORICAL)
    )
  )

  val SEGMENTATION = Array[Array[Schema[Any]]](
    // http://www.youtube.com,1000,100,100,920,2,advertisement
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    ),
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val COMMUTE = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    ),
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val COMMUTEFULL = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    ),
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val DELAYS = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    ),
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val CUSTOMERS = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER)
    ),
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )

  val FAULTS = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val STUDENTGRADE = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

  val MOVIERATING = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val NUMBERSERIES = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val AGEANALYSIS = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val WORDCOUNT = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER)
    )
  )

    val EXTERNALCALL = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER)
    )
  )

    val FINDSALARY = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val INSIDECIRCLE = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val MAPSTRING = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_OTHER)
    )
  )

    val INCOMEAGGREGATION = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL)
    )
  )

    val LOANTYPE = Array[Array[Schema[Any]]](
    Array(
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_NUMERICAL),
      new Schema(Schema.TYPE_OTHER)
    )
  )


}

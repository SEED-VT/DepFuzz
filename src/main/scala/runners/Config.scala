package runners

import examples.{benchmarks, faulty, fuzzable, monitored}
import fuzzer.{ProvInfo, Schema}
import schemas.BenchmarkSchemas

import scala.collection.mutable.ListBuffer

object Config {

  val scalaVersion = 2.12
  val maxSamples = 5
  val maxRepeats = 1
  val percentageProv = 0.1f
  val iterations = 10
  val fuzzDuration = 10 // 86400 // duration in seconds
  val benchmarkName = "WebpageSegmentation"
  val resultsDir = s"./target/fuzzer-results/$benchmarkName"
  val faultTest = true
  val deepFaults = false
  val seedType = "weak" //either full, reduced or weak
  val benchmarkClass = s"examples.${if (faultTest) "faulty" else "fuzzable"}.$benchmarkName"
  val mutateProbs: Array[Float] = Array( // 0:M1, 1:M2 ... 5:M6
    0.9f, // Data
    0.02f, // Data
    0.02f, // Format
    0.02f, // Format
    0.02f, // Format
    0.02f) // Format


  val mutateProbsProvFuzz: Array[Float] = Array( // 0:M1, 1:M2 ... 5:M6
    0.9f, // Data
    0.000000000000000000f, // Data
    0.000000000000000000f, // Format
    0.000000000000000000f, // Format
    0.000000000000000000f, // Format
    0.000000000000000000f) // Format

//
//  val provInfos: Map[String, ProvInfo] = Map (
//    "Customers" -> new ProvInfo(ListBuffer(ListBuffer((0,0,249), (1,1,83)), ListBuffer((0,0,381), (1,1,127)), ListBuffer((0,0,327), (1,1,109)), ListBuffer((0,0,45), (1,1,15)), ListBuffer((0,0,255), (1,1,85)), ListBuffer((1,1,10), (0,0,30)), ListBuffer((1,1,1), (0,0,3)), ListBuffer((1,1,125), (0,0,375)), ListBuffer((1,1,126), (0,0,378)), ListBuffer((1,1,107), (0,0,321)))),
//    "Delays" ->  new ProvInfo(ListBuffer(ListBuffer((0,0,9), (1,0,9)), ListBuffer((0,0,107), (1,0,107)), ListBuffer((0,0,2), (1,0,2)), ListBuffer((0,0,217), (1,0,217)), ListBuffer((0,0,371), (1,0,371)), ListBuffer((1,2,371), (1,1,371), (0,2,371), (0,1,371)), ListBuffer((1,2,392), (1,1,392), (0,2,392), (0,1,392)), ListBuffer((1,2,103), (1,1,103), (0,2,103), (0,1,103)), ListBuffer((1,2,82), (1,1,82), (0,2,82), (0,1,82)), ListBuffer((1,2,212), (1,1,212), (0,2,212), (0,1,212)))),
//    "FlightDistance" ->  new ProvInfo(ListBuffer(ListBuffer((0,4,17), (1,0,78)), ListBuffer((0,4,23), (1,0,7)), ListBuffer((0,4,12), (1,0,66)), ListBuffer((0,4,25), (1,0,60)), ListBuffer((0,4,22), (1,0,8)), ListBuffer((0,5,453), (1,0,23)), ListBuffer((0,5,25), (1,0,39)), ListBuffer((0,5,3), (1,0,86)), ListBuffer((0,0,298)), ListBuffer((0,0,278)), ListBuffer((0,0,444)), ListBuffer((0,0,307)), ListBuffer((0,0,107)))),
//    "DeliveryFaults" ->  new ProvInfo(ListBuffer(ListBuffer((0,0,2)), ListBuffer((0,0,13)), ListBuffer((0,0,16)), ListBuffer((0,0,27)), ListBuffer((0,0,28)))),
//    "WebpageSegmentation" ->  new ProvInfo(ListBuffer(ListBuffer((1,0,10)), ListBuffer((1,0,14)), ListBuffer((0,0,0), (0,5,0), (0,6,0), (1,0,5), (1,0,6), (1,5,5), (1,5,6), (1,6,5), (1,6,6)), ListBuffer((0,0,0), (0,5,0), (0,6,0), (1,0,5), (1,0,7), (1,5,5), (1,5,7), (1,6,5), (1,6,7)), ListBuffer((1,4,5), (1,1,5)), ListBuffer((1,4,6), (1,1,6)), ListBuffer((1,4,7), (1,1,7)), ListBuffer((1,4,8), (1,1,8)), ListBuffer((1,4,9), (1,1,9)), ListBuffer((1,2,8), (1,3,8)), ListBuffer((1,2,9), (1,3,9)))),
//    "CommuteType" ->  new ProvInfo(ListBuffer(ListBuffer((0,3,0)), ListBuffer((0,3,1)), ListBuffer((0,3,2)), ListBuffer((0,3,3)), ListBuffer((0,3,4)))),
//    "CommuteTypeFull" ->  new ProvInfo(ListBuffer(ListBuffer((0,1,0), (1,0,0)), ListBuffer((0,1,0), (1,0,1)), ListBuffer((0,1,0), (1,0,2)), ListBuffer((0,1,0), (1,0,3)), ListBuffer((0,1,0), (1,0,4)))),
//    "AgeAnalysis" ->  new ProvInfo(ListBuffer(ListBuffer((0,1,5)), ListBuffer((0,1,0)), ListBuffer((0,1,6)), ListBuffer((0,1,1)), ListBuffer((0,1,7)), ListBuffer((0,1,2)), ListBuffer((0,1,8)), ListBuffer((0,1,3)), ListBuffer((0,1,4)))),
//    "ExternalCall" ->  new ProvInfo(ListBuffer(ListBuffer((0,0,0)))), // temp
//    "FindSalary" ->  new ProvInfo(ListBuffer(ListBuffer((0,0,0)),ListBuffer((0,0,1)),ListBuffer((0,0,2)))),
//    "IncomeAggregation" ->  new ProvInfo(ListBuffer(ListBuffer((0,1,0)), ListBuffer((0,1,1)), ListBuffer((0,1,2)), ListBuffer((0,1,3)), ListBuffer((0,1,4)))),
//    "InsideCircle" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)),ListBuffer((0,1,0)),ListBuffer((0,2,0)))),
//    "LoanType" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)))), // temp
//    "MapString" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)))), // temp
//    "MovieRating" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)),ListBuffer((0,1,0)),ListBuffer((0,0,1)),ListBuffer((0,1,1)))),
//    "NumberSeries" -> new ProvInfo(ListBuffer(ListBuffer((0,1,4)), ListBuffer((0,1,0)), ListBuffer((0,1,5)), ListBuffer((0,1,1)), ListBuffer((0,1,2)), ListBuffer((0,1,6)), ListBuffer((0,1,3)))),
//    "StudentGrade" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)), ListBuffer((0,1,0)))),
//    "WordCount" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)))) // temp
//  )
//
//  val provInfosForWeak: Map[String, ProvInfo] = Map (
//    "Customers" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0), (1,1,0)))),
//    "Delays" ->  new ProvInfo(ListBuffer(ListBuffer((0,0,0), (1,0,0)), ListBuffer((0,0,0), (1,0,0)), ListBuffer((0,0,0), (1,0,0)), ListBuffer((0,0,0), (1,0,0)), ListBuffer((0,0,0), (1,0,0)), ListBuffer((1,2,0), (1,1,0), (0,2,0), (0,1,0)), ListBuffer((1,2,0), (1,1,0), (0,2,0), (0,1,0)), ListBuffer((1,2,0), (1,1,0), (0,2,0), (0,1,0)), ListBuffer((1,2,0), (1,1,0), (0,2,0), (0,1,0)), ListBuffer((1,2,0), (1,1,0), (0,2,0), (0,1,0)))),
//    "FlightDistance" ->  new ProvInfo(ListBuffer(ListBuffer((0,4,0), (1,0,0)), ListBuffer((0,4,0), (1,0,0)), ListBuffer((0,4,0), (1,0,0)), ListBuffer((0,4,0), (1,0,0)), ListBuffer((0,4,0), (1,0,0)), ListBuffer((0,5,0), (1,0,0)), ListBuffer((0,5,0), (1,0,0)), ListBuffer((0,5,0), (1,0,0)), ListBuffer((0,0,0)), ListBuffer((0,0,0)), ListBuffer((0,0,0)), ListBuffer((0,0,0)), ListBuffer((0,0,0)))),
//    "DeliveryFaults" ->  new ProvInfo(ListBuffer(ListBuffer((0,0,0)), ListBuffer((0,0,0)), ListBuffer((0,0,0)), ListBuffer((0,0,0)), ListBuffer((0,0,0)))),
//    "WebpageSegmentation" ->  new ProvInfo(ListBuffer(ListBuffer((1,0,0)), ListBuffer((1,0,0)), ListBuffer((0,0,0), (0,5,0), (0,6,0), (1,6,0), (1,5,0), (1,0,0)))),
//    "CommuteType" ->  new ProvInfo(ListBuffer(ListBuffer((0,3,0)), ListBuffer((0,3,0)), ListBuffer((0,3,0)), ListBuffer((0,3,0)), ListBuffer((0,3,0)))),
//    "CommuteTypeFull" ->  new ProvInfo(),
//    "AgeAnalysis" ->  new ProvInfo(ListBuffer(ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)))),
//    "ExternalCall" ->  new ProvInfo(ListBuffer(ListBuffer((0,0,0)))), // temp
//    "FindSalary" ->  new ProvInfo(ListBuffer(ListBuffer((0,0,0)),ListBuffer((0,0,0)),ListBuffer((0,0,0)))),
//    "IncomeAggregation" ->  new ProvInfo(ListBuffer(ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)))),
//    "InsideCircle" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)),ListBuffer((0,1,0)),ListBuffer((0,2,0)))),
//    "LoanType" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)))), // temp
//    "MapString" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)))), // temp
//    "MovieRating" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)),ListBuffer((0,1,0)),ListBuffer((0,0,0)),ListBuffer((0,1,0)))),
//    "NumberSeries" -> new ProvInfo(ListBuffer(ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)), ListBuffer((0,1,0)))),
//    "StudentGrade" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)), ListBuffer((0,1,0)))),
//    "WordCount" -> new ProvInfo(ListBuffer(ListBuffer((0,0,0)))) // temp
//  )
  val mapInputFilesWeak: Map[String, Array[String]] = Map(
    "FlightDistance" -> Array("seeds/weak_seed/FlightDistance/flights", "seeds/weak_seed/FlightDistance/airports_data"),
    "WebpageSegmentation" -> Array("seeds/weak_seed/webpage_segmentation/before", "seeds/weak_seed/webpage_segmentation/after"),
    "CommuteType" -> Array("seeds/weak_seed/commute/trips"),
    "CommuteTypeFull" ->  Array("seeds/weak_seed/commute_type/trips", "seeds/weak_seed/commute_type/locations"),
    "Delays" -> Array("seeds/weak_seed/delays/station1", "seeds/weak_seed/delays/station2"),
    "Customers" -> Array("seeds/weak_seed/orders/customers", "seeds/weak_seed/orders/orders"),
    "DeliveryFaults" -> Array("seeds/weak_seed/deliveryfaults/deliveries"),
    "StudentGrade" -> Array("seeds/weak_seed/studentgrade/grades"),
    "MovieRating" -> Array("seeds/weak_seed/movierating/ratings"),
    "NumberSeries" -> Array("seeds/weak_seed/numberseries/numbers"),
    "AgeAnalysis" -> Array("seeds/weak_seed/ageanalysis/ages"),
    "WordCount" -> Array("seeds/weak_seed/wordcount/words"),
    "ExternalCall" -> Array("seeds/weak_seed/externalcall/calls"),
    "FindSalary" -> Array("seeds/weak_seed/findsalary/salaries"),
    "InsideCircle" -> Array("seeds/weak_seed/insidecircle/circles"),
    "MapString" -> Array("seeds/weak_seed/mapstring/strings"),
    "IncomeAggregation" -> Array("seeds/weak_seed/incomeaggregation/income"),
    "LoanType" -> Array("seeds/weak_seed/loantype/info")
  )

  val mapInputFilesFull: Map[String, Array[String]] = Map(
    "WebpageSegmentation" -> Array("seeds/full_data/webpage_segmentation/before", "seeds/full_data/webpage_segmentation/after"),
    "CommuteType" -> Array("seeds/full_data/trips"),
    "CommuteTypeFull" ->  Array("seeds/full_data/commute_type/trips","seeds/full_data/commute_type/locations"),
    "Customers" -> Array("seeds/full_data/customers/customers", "seeds/full_data/customers/orders"),
    "FlightDistance" -> Array("seeds/full_data/LongFlights/flights", "seeds/full_data/LongFlights/airports"),
    "DeliveryFaults" -> Array("seeds/full_data/deliveries"),
    "Delays" -> Array("seeds/full_data/delays/station1", "seeds/full_data/delays/station2")
  )

  val mapInputFilesReduced: Map[String, Array[String]] = Map(
    "WebpageSegmentation" -> Array("seeds/reduced_data/webpage_segmentation/before", "seeds/reduced_data/webpage_segmentation/after"),
    "CommuteType" -> Array("seeds/reduced_data/trips"),
    "CommuteTypeFull" ->  Array("seeds/reduced_data/commute_type/trips", "seeds/reduced_data/commute_type/locations"),
    "Customers" -> Array("seeds/reduced_data/customers/customers", "seeds/reduced_data/customers/orders"),
    "FlightDistance" -> Array("seeds/reduced_data/LongFlights/flights", "seeds/reduced_data/LongFlights/airports"),
    "DeliveryFaults" -> Array("seeds/reduced_data/deliveries"),
    "Delays" -> Array("seeds/reduced_data/delays/station1", "seeds/reduced_data/delays/station2"),
    "AgeAnalysis" -> Array("seeds/reduced_data/ageanalysis/ages"),
    "ExternalCall" -> Array("seeds/reduced_data/externalcall/calls"),
    "IncomeAggregation" -> Array("seeds/reduced_data/incomeaggregation/income"),
    "InsideCircle" -> Array("seeds/reduced_data/insidecircle/circles"),
    "MapString" -> Array("seeds/reduced_data/mapstring/strings"),
    "MovieRating" -> Array("seeds/reduced_data/movierating/ratings"),
    "NumberSeries" -> Array("seeds/reduced_data/numberseries/numbers"),
    "StudentGrade" -> Array("seeds/reduced_data/studentgrade/grades"),
    "FindSalary" -> Array("seeds/reduced_data/findsalary/salaries"),
    "LoanType" -> Array("seeds/reduced_data/loantype/info"),
    "WordCount" -> Array("seeds/reduced_data/wordcount/words")
  )

  val Some(mapInputFiles) = Map(
    "weak" -> mapInputFilesWeak,
    "reduced" -> mapInputFilesReduced,
    "full" -> mapInputFilesFull
  ).get(seedType)


  def Switch(normal: Array[String] => Unit, faulty: Array[String] => Unit, switch: Boolean): Array[String] => Unit = {
    if (switch) faulty else throw new Exception("You want to fuzz the faulty one") // normal
  }

  val mapFunFuzzables: Map[String, Array[String] => Unit] = Map[String, Array[String] => Unit](elems =
    "FlightDistance" -> Switch(fuzzable.FlightDistance.main, faulty.FlightDistance.main, faultTest),
    "WebpageSegmentation" -> Switch(fuzzable.WebpageSegmentation.main, faulty.WebpageSegmentation.main, faultTest),
    "CommuteType" -> Switch(fuzzable.CommuteType.main, faulty.CommuteType.main, faultTest),
    "CommuteTypeFull" ->  Switch(null, faulty.CommuteTypeFull.main, faultTest),
    "Delays" -> Switch(fuzzable.Delays.main, faulty.Delays.main, faultTest),
    "Customers" -> Switch(fuzzable.Customers.main, faulty.Customers.main, faultTest),
    "DeliveryFaults" -> Switch(fuzzable.DeliveryFaults.main, faulty.DeliveryFaults.main, faultTest),
    "StudentGrade" -> Switch(null, faulty.StudentGrade.main, faultTest),
    "MovieRating" -> Switch(null, faulty.MovieRating.main, faultTest),
    "NumberSeries" -> Switch(null, faulty.NumberSeries.main, faultTest),
    "AgeAnalysis" -> Switch(null, faulty.AgeAnalysis.main, faultTest),
    "WordCount" -> Switch(null, faulty.WordCount.main, faultTest),
    "ExternalCall" -> Switch(null, faulty.ExternalCall.main, faultTest),
    "FindSalary" -> Switch(null, faulty.FindSalary.main, faultTest),
    "InsideCircle" -> Switch(null, faulty.InsideCircle.main, faultTest),
    "MapString" -> Switch(null, faulty.MapString.main, faultTest),
    "IncomeAggregation" -> Switch(null, faulty.IncomeAggregation.main, faultTest),
    "LoanType" -> Switch(null, faulty.LoanType.main, faultTest)
  )


  val mapFunSpark: Map[String, Array[String] => Unit] = Map[String, Array[String] => Unit](elems =
    "FlightDistance" -> benchmarks.FlightDistance.main,
    "WebpageSegmentation" -> benchmarks.WebpageSegmentation.main,
    "CommuteType" -> benchmarks.CommuteType.main,
    "Delays" -> benchmarks.Delays.main,
    "Customers" -> benchmarks.Customers.main,
    "DeliveryFaults" -> benchmarks.DeliveryFaults.main
  )

  val mapFunProbeAble: Map[String, Array[String] => ProvInfo] = Map[String, Array[String] => ProvInfo](elems =
    "FlightDistance" -> monitored.FlightDistance.main,
    "WebpageSegmentation" -> monitored.WebpageSegmentation.main,
    "CommuteType" -> monitored.CommuteType.main,
    "CommuteTypeFull" -> monitored.CommuteTypeFull.main,
    "Delays" -> monitored.Delays.main,
    "Customers" -> monitored.Customers.main,
    "DeliveryFaults" -> monitored.DeliveryFaults.main,
    "AgeAnalysis" -> monitored.AgeAnalysis.main,
    "IncomeAggregation" -> monitored.IncomeAggregation.main,
    "ExternalCall" -> monitored.ExternalCall.main,
    "InsideCircle" -> monitored.InsideCircle.main,
    "MovieRating" -> monitored.MovieRating.main,
    "FindSalary" -> monitored.FindSalary.main,
    "LoanType" -> monitored.LoanType.main,
    "NumberSeries" -> monitored.NumberSeries.main,
    "StudentGrade" -> monitored.StudentGrade.main
  )

  val mapSchemas: Map[String, Array[Array[Schema[Any]]]] = Map[String, Array[Array[Schema[Any]]]](elems =
    "FlightDistance" -> BenchmarkSchemas.SYNTHETIC3,
    "WebpageSegmentation" -> BenchmarkSchemas.SEGMENTATION,
    "CommuteType" -> BenchmarkSchemas.COMMUTE,
    "CommuteTypeFull" -> BenchmarkSchemas.COMMUTEFULL,
    "Delays" -> BenchmarkSchemas.DELAYS,
    "Customers" -> BenchmarkSchemas.CUSTOMERS,
    "DeliveryFaults" -> BenchmarkSchemas.FAULTS,
    "StudentGrade" -> BenchmarkSchemas.STUDENTGRADE,
    "MovieRating" -> BenchmarkSchemas.MOVIERATING,
    "NumberSeries" -> BenchmarkSchemas.NUMBERSERIES,
    "AgeAnalysis" -> BenchmarkSchemas.AGEANALYSIS,
    "WordCount" -> BenchmarkSchemas.WORDCOUNT,
    "ExternalCall" -> BenchmarkSchemas.EXTERNALCALL,
    "FindSalary" -> BenchmarkSchemas.FINDSALARY,
    "InsideCircle" -> BenchmarkSchemas.INSIDECIRCLE,
    "MapString" -> BenchmarkSchemas.MAPSTRING,
    "IncomeAggregation" -> BenchmarkSchemas.INCOMEAGGREGATION,
    "LoanType" -> BenchmarkSchemas.LOANTYPE
  )

  val mapErrorCountAll: Map[String, Int] = Map[String, Int](elems =
    "FlightDistance" -> 7,
    "WebpageSegmentation" -> 10,
    "CommuteType" -> 6,
    "Delays" -> 10,
    "Customers" -> 10,
    "DeliveryFaults" -> 7
  )

  val mapErrorCountDeep: Map[String, Int] = Map[String, Int](elems =
    "FlightDistance" -> 3,
    "WebpageSegmentation" -> 5,
    "CommuteType" -> 3,
    "Delays" -> 6,
    "Customers" -> 3,
    "DeliveryFaults" -> 2
  )

  val Some(map_err_count) = Map(elems =
    false -> mapErrorCountAll,
    true -> mapErrorCountDeep
  ).get(deepFaults)
}

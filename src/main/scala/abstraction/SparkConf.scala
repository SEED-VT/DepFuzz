package abstraction

class SparkConf {
  def setAppName(str: String): abstraction.SparkConf = { new SparkConf() }

  def setMaster(str: String): abstraction.SparkConf = { new SparkConf() }

  def set(str: String, str2: String): abstraction.SparkConf = { new SparkConf() }

}

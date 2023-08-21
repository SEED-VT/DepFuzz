package testfiles

import java.lang.reflect.{Method, Modifier}

object Test4 {

  def invokeMethod(className: String, functionName: String, args: Any*): Option[Any] = {
    try {
      val clazz = Class.forName(className)
      val method = findMethod(clazz, functionName, args.map(_.getClass): _*)
      method.setAccessible(true)
      val instance = if (Modifier.isStatic(method.getModifiers)) null else clazz.newInstance()
      Some(method.invoke(instance, args.asInstanceOf[Seq[AnyRef]]: _*))
    } catch {
      case ex: Exception =>
        println(s"An error occurred while invoking the method: ${ex}")
        None
    }
  }

  def findMethod(clazz: Class[_], methodName: String, argTypes: Class[_]*): Method = {
    clazz
      .getDeclaredMethods.filter(_.getName == methodName)
      .head
  }

  def main(args: Array[String]): Unit = {
    val result1 = invokeMethod("testfiles.MyClass", "addNumbers", 3, 4)
    result1.foreach(println) // Output: 7

    val result2 = invokeMethod("testfiles.MyClass", "printMessage", "Hello, World!")

    invokeMethod("examples.benchmarks.WebpageSegmentation", "main", Array("before","after").map(s => s"seeds/reduced_data/webpage_segmentation/$s"))

//     Output: Hello, World!
  }
}

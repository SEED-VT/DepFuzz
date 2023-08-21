package transformers

object TestCases {

  val testCases = Map(
    "predicateTest1" ->
      """
        |object predicateTest1 {
        |
        |  def something(findme: Int) = {}
        |  def main(args: Array[String]): Unit = {
        |   val x = 3
        |   if(x < 5) {
        |     println("do this")
        |   } else {
        |     println("do that")
        |   }
        |  }
        |}
        |""".stripMargin,
    "predicateTest2" ->
      """
        |package transformers.testout
        |
        |import org.apache.spark.{SparkConf, SparkContext}
        |
        |object predicateTest2 {
        |
        |  def main(args: Array[String]): Unit = {
        |    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
        |    val ctx = new SparkContext(sparkConf)
        |    val data1 = ctx.textFile("dummydata/predicateTest2").map{arr => arr.split(",")}
        |    val data2 = ctx.textFile("dummydata/predicateTest2").map{arr => arr.split(",")}
        |
        |    val d1 = data1.map(arr => (arr.head, arr.tail))
        |    val d2 = data2.map(arr => (arr.head, arr.tail))
        |
        |    val d3 = d1.join(d2).filter{ case (k, (_, _)) => k.toInt > 3 && k.toInt < 6}
        |    d3.groupByKey().foreach(println)
        |  }
        |
        |}
        |""".stripMargin,

    "fullTestCase1" ->
      """
        |package transformers.testout
        |
        |import org.apache.spark.{SparkConf, SparkContext}
        |
        |object fullTestCase1 {
        |
        |  def main(args: Array[String]): Unit = {
        |    println(s"webpage WebpageSegmentation args ${args.mkString(",")}")
        |    val sparkConf = new SparkConf()
        |    sparkConf.setMaster("local[6]")
        |    sparkConf.setAppName("Webpage Segmentation").set("spark.executor.memory", "2g")
        |    val before_data = args(0) // "datasets/fuzzing_seeds/webpage_segmentation/before"
        |    val after_data = args(1) // "datasets/fuzzing_seeds/webpage_segmentation/after"
        |    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
        |    ctx.setLogLevel("ERROR")
        |    val before = ctx.textFile(before_data).map(_.split(','))
        |    val after = ctx.textFile(after_data).map(_.split(','))
        |
        |    val boxes_before = before.map(r => (s"${r(0)}*${r(r.length-2)}*${r.last}", (r(0), r.slice(1, r.length-2).map(_.toInt).toVector)))
        |    val boxes_after = after.map(r => (s"${r(0)}*${r(r.length-2)}*${r.last}", (r(0), r.slice(1, r.length-2).map(_.toInt).toVector)))
        |    val boxes_after_by_site = after.map(r => (r(0), (r.slice(1, r.length-2).map(_.toInt).toVector, r(r.length-2), r.last)))
        |      .groupByKey()
        |
        |    val pairs = boxes_before.join(boxes_after)
        |    val changed = pairs
        |      .filter{
        |        case (_, ((_, v1), (_, v2))) => !v1.equals(v2)
        |      }
        |      .map{
        |        case (k, (_, (url, a))) =>
        |          val Array(_, cid, ctype) = k.split('*')
        |          (url, (a, cid, ctype))
        |      }
        |    val inter = changed.join(boxes_after_by_site)
        |    inter.map{
        |      case (url, ((box1, _, _), lst)) => (url, lst.map{case (box, _, _) => box}.map(intersects(_, box1)))
        |    }.collect()
        |  }
        |
        |  def intersects(rect1: IndexedSeq[Int],
        |                 rect2: IndexedSeq[Int]): Option[(Int, Int, Int, Int)] = {
        |
        |    val IndexedSeq(aSWx, aSWy, aHeight, aWidth) = rect1
        |    val IndexedSeq(bSWx, bSWy, bHeight, bWidth) = rect2
        |    val endpointax = aSWx + aWidth;
        |    val startpointax = aSWx;
        |    val endpointay = aSWy + aHeight;
        |    val startpointay = aSWy;
        |    val endpointbx = bSWx + bWidth;
        |    val startpointbx = bSWx;
        |    val endpointby = bSWy + bHeight;
        |    val startpointby = bSWy;
        |
        |
        |    if ((endpointax < startpointbx) && (startpointax < startpointbx) ){
        |      return None;
        |    }
        |    if ((endpointbx < startpointax) && (startpointbx < startpointax)){
        |      return None;
        |    }
        |
        |    if ((endpointby < startpointay) && (startpointby < startpointay)){
        |      return None;
        |    }
        |
        |    if ((endpointay < startpointby) && (startpointay < startpointby)){
        |      return None;
        |    }
        |
        |    if (startpointay > endpointby){
        |      return None;
        |    }
        |
        |    var iSWx, iSWy, iWidth, iHeight  = 0
        |
        |    if ((startpointax <= startpointbx) && (endpointbx <= endpointax)) {
        |      iSWx  = startpointbx;
        |      iSWy = if (startpointay < startpointby) startpointby else startpointay;
        |      iWidth = bWidth;
        |      val top = if (endpointby < endpointay) endpointby else endpointay;
        |      iHeight = (top - iSWy);
        |    }
        |    else if ((startpointbx <= startpointax) && (endpointax <= endpointbx)) {
        |      iSWx  = startpointax;
        |      iSWy = if (startpointay < startpointby) startpointby else startpointay;
        |      iWidth = aWidth;
        |      val top = if (endpointby < endpointay) endpointby  else endpointay;
        |      iHeight = (top - iSWy);
        |    }
        |    else if ((startpointax >= startpointbx) && (startpointax <= endpointbx)) {
        |      iSWx  = startpointax;
        |      iSWy = if (startpointay > startpointby) startpointay else startpointby;
        |      iWidth = (endpointbx - startpointax);
        |      val top = if (endpointby < endpointay) endpointby  else endpointay;
        |      iHeight = (top - iSWy);
        |    }
        |    else if ((startpointbx >= startpointax) && (startpointbx <= endpointax)) {
        |      iSWx  = startpointbx;
        |      iSWy = if (startpointay > startpointby) startpointay else startpointby;
        |      iWidth = (endpointax - startpointbx);
        |      val top = if (endpointby < endpointay) endpointby  else endpointay;
        |      iHeight = (top - iSWy);
        |    }
        |
        |    Some((iSWx, iSWy, iHeight, iWidth))
        |  }
        |
        |}
        |""".stripMargin
  )
}

/*
  private val provVars = ListBuffer[ListBuffer[String]](ListBuffer[String]())
  private val tripleQuotes = """""""*3
  var in_predicate = false
  var nest_level = 0
  var b_id = 0

  private def joinList(l: ListBuffer[String]) : String = l.fold(""){(acc, e) => acc+","+e}

  private def commaSep(names: ListBuffer[ListBuffer[String]]): String = {
    var vars = ""
    names.foreach({l => {
      l.foreach({n => vars += n + ","})
    }})
    vars.dropRight(1)
  }
  private def spreadVars(names: ListBuffer[ListBuffer[String]]): String ={
    println(names)
    val this_pred_vars = names.last.mkString(",")
    val prev_vars = names.drop(1).dropRight(1).fold(""){(acc, e) => acc+","+joinList(e.asInstanceOf[ListBuffer[String]])}
    s"(List[Any](${names.last.mkString(",")}), List[Any](${commaSep(names.init)}))"
  }

  private def unquoteBlock(exp: String): String = {
    exp.replaceAll("xxd_qq ", "\\$")
  }

  private def removeDollars(exp: Term): Term = {
    exp match {
      case node @ Term.Name(name) => if (name.startsWith("$")) Term.Name(name.tail) else node
      case Term.ApplyInfix(lhs, op, targs, args) => Term.ApplyInfix(removeDollars(lhs), op, targs, args.map(removeDollars))
      case _ => exp
    }
  }

  private def remove_marker(exp: Term): Term = {
    exp match {
      case Term.Apply(Term.Name(name), List(Term.Block(List(n)))) =>
        if (name.equals("xxd_qq")) {
          n.asInstanceOf[Term]
        } else {
          exp
        }
      case _ => removeDollars(exp)
    }
  }
  private def wrap(exp: Term, prov: ListBuffer[ListBuffer[String]]): Term = {
    println(s"wrapping $exp")
    //Using triple quotes for cases where the predicate contains string literals
    val quasi_exp = s"""q${tripleQuotes}${unquoteBlock(exp.toString())}${tripleQuotes}"""
    println(s"STRUCTURE $exp == ${exp.structure}")
    val wrapped = s"_root_.transformers.BranchTracker.provWrapper(${remove_marker(exp).toString()}, $quasi_exp, ${spreadVars(prov)}, $b_id)"
    b_id += 1
    println(wrapped)
    wrapped.parse[Term].get
  }

  private def unquote(name: String): String = "$"+name

  private def unquoteNonVars(node: meta.Tree): meta.Tree ={
    node match {
      case n @ Term.Apply(_) => Term.Apply(Term.Name("xxd_qq"), List[Term](Term.Block(List[Term](n))))
      case n @ Term.Select(_) => Term.Apply(Term.Name("xxd_qq"), List[Term](Term.Block(List[Term](n))))
      case _ => super.apply(node)
    }
  }

  val math_funcs = Array("sin", "cos", "tan", "pow")
  val supported_aggops = ListBuffer[String]("join", "reduceByKey", "groupByKey")
  val aggops_map = Map(
    "join" -> "_root_.transformers.BranchTracker.joinWrapper".parse[Term].get,
    "reduceByKey" -> "_root_.transformers.BranchTracker.reduceByKeyWrapper".parse[Term].get,
    "groupByKey" -> "_root_.transformers.BranchTracker.groupByKeyWrapper".parse[Term].get)
  val aggops_ids = mutable.Map("join" -> -1, "reduceByKey" -> -1, "groupByKey" -> -1)

  override def apply(tree: meta.Tree): meta.Tree = {
    tree match {
      case Pkg(_, code) => super.apply(Pkg("examples.monitored".parse[Term].get.asInstanceOf[Term.Ref], code))
      case node @ Defn.Def(a, b @ Term.Name(fname), c, d, e, Term.Block(code)) =>
        println("case Defn.Def met for ", node)
        if (fname.equals("main")) {
          val inst = code :+ "_root_.transformers.BranchTracker.finalizeProvenance()".parse[Term].get
          return Defn.Def(a, b, c, d, e, super.apply(Term.Block(inst)).asInstanceOf[Term])
        }
        super.apply(node)
      case Defn.Object(mod, Term.Name(name), template) => super.apply(Defn.Object(mod, Term.Name("probe_" + name), template))
      case node @ Term.Apply(Term.Select(Term.Name(ds), Term.Name(name)), lst_args) =>
        println("case Term.Apply(Term.Select... met")
        if(supported_aggops.contains(name)){
          aggops_ids.update(name, aggops_ids(name) + 1)
          return Term.Apply(aggops_map(name), Lit.Int(aggops_ids(name))::Term.Name(ds)::lst_args)
        }
        super.apply(node)
      case node @ Term.Name(name) =>
        println("case Term.Name met: ", node)
        if (in_predicate && name.matches("^[a-zA-Z_$][a-zA-Z_$0-9]*$")) {
          provVars(nest_level) += name
          Term.Name(unquote(name))
        } else
          super.apply(node)
      case node @ Term.Apply(_) =>
        println("case Term.Apply met")
        if (in_predicate) unquoteNonVars(node) else super.apply(node)
      case node @ Term.Select(_) =>
        println("case Term.Select met: ", node)
        if (in_predicate) unquoteNonVars(node) else super.apply(node)
      case Term.If(exp, if_body, else_body) =>
        println("case Term.If met")
        //        println("if condition hit")
        nest_level += 1
        provVars += ListBuffer[String]()
        in_predicate = true
        val e = apply(exp)
        in_predicate = false
        val wrapped = wrap(e.asInstanceOf[Term], provVars)
        val ifb = apply(if_body).asInstanceOf[Term]
        val elb = apply(else_body).asInstanceOf[Term]
        provVars.remove(nest_level)
        nest_level -= 1
        Term.If(wrapped, ifb, elb)
      case Defn.Val(lst @ _) =>
        val (a,lhs,c,rhs) = lst
        println("case Defn.Val met:", rhs.structure)
        Defn.Val(a,lhs,c,apply(rhs).asInstanceOf[Term])
      case node =>
        println("general case met for ", node)
        super.apply(node)
    }
  }
 */
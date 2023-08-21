package transformers

import java.nio.file.Files
import scala.meta.{Defn, Import, Importee, Importer, Init, Input, Name, Source, Stat, Term, Transformer, Tree, Type, XtensionParseInputLike}
import transformers.Constants

object MonitorAttacher extends Transformer {

  def treeFromFile(p: String): Tree = {
    val path = java.nio.file.Paths.get(p)
    val bytes = java.nio.file.Files.readAllBytes(path)
    val text = new String(bytes, "UTF-8")
    val input = Input.VirtualFile(path.toString, text)
    input.parse[Source].get
  }

  def writeTransformed(code: String, p: String) = {
    val path = java.nio.file.Paths.get(p)
    Files.write(path, code.getBytes())
  }

  def attachMonitors(tree: Tree): Tree = {
    this.apply(tree)
  }

  def extractVars(tree: Tree): List[String] = {
    val name = tree match {
      case Term.Select(qual, name) => s"${qual.toString()}.${name.value}"
      case Term.Name(name) => name
      case _ => ""
    }
    (name +: tree.children.flatMap(c => extractVars(c))).filter(_.matches("^[a-zA-Z_$][a-zA-Z_$0-9]*$"))
  }

  def toListStatement(vars: List[String]): String = {
    s"List[Any](${vars.mkString(",")})"
  }
  def attachPredicateMonitor(predicate: Term, id: Int, prevVars: List[String] = List()): (Term, Int) = {
    ((s"${Constants.MAP_TRANSFORMS(Constants.KEY_PREDICATE)}" +
      s"(${predicate.toString()}, (${toListStatement(extractVars(predicate))}, ${toListStatement(prevVars)}), $id)")
        .parse[Term].get, id+1)
  }

  def attachDFOMonitor(dfo: Term, id: Int): (Term, Int) = {
    val Term.Apply(Term.Select(rddName, Term.Name(dfoName)), args) = dfo
    (dfoName match {
      case Constants.KEY_JOIN => s"${Constants.MAP_TRANSFORMS(Constants.KEY_JOIN)}($rddName, ${args.mkString(",")}, $id)".parse[Term].get
      case Constants.KEY_GBK => s"${Constants.MAP_TRANSFORMS(Constants.KEY_GBK)}($rddName, $id)".parse[Term].get
      case Constants.KEY_RBK => s"${Constants.MAP_TRANSFORMS(Constants.KEY_RBK)}($rddName, ${args.mkString(",")}, $id)".parse[Term].get
//      case Constants.KEY_FILTER => s"${Constants.MAP_TRANSFORMS(Constants.KEY_FILTER)}($rddName, ${args.mkString(",")}, $id)".parse[Term].get
      case _ => dfo
    }, id+1)
  }

  def insertAtEndOfFunction(mainFunc: Defn, statement: Stat): Defn = {
    val Defn.Def(mods, name, tparams, paramss, decltpe, Term.Block(code)) = mainFunc
    Defn.Def(mods, name, tparams, paramss, decltpe, Term.Block(code :+ statement))
  }

  def modifySparkContext(term: Term): Term = {
    Term.New(Init(Type.Name("SparkContextWithDP"), Name(""), List(List(term))))
  }

  def modifyDatasetLoading(term: Term): Term = {
    val Term.Apply(Term.Select(Term.Apply(Term.Select(prefix, _), args), Term.Name("map")), List(parseFunc)) = term
    Term.Apply(Term.Select(prefix, Term.Name("textFileProv")), args :+ parseFunc)
  }

  def addImports(imports: List[Importer]): List[Importer] = {
    imports ++ List(
      "sparkwrapper.SparkContextWithDP",
      "taintedprimitives._",
      "taintedprimitives.SymImplicits._"
    ).map(_.parse[Importer].get)
//      Importer(Term.Name("sparkwrapper"), List(Importee.Name(Name("SparkContextWithDP"))))
  }

  var id = 0

  override def apply(tree: Tree): Tree = {
    tree match {
      case Import(imports) =>
        Import(addImports(imports))
      case node @ Term.New(Init(Type.Name("SparkContext"), _, _)) =>
        modifySparkContext(node)
      case node @ Term.Apply(Term.Select(Term.Apply(Term.Select(_, Term.Name("textFile")), _), Term.Name("map")), _) =>
        modifyDatasetLoading(node)
      case node @ Term.Apply(Term.Select(_, name), _) if Constants.MAP_TRANSFORMS.contains(name.value) =>
        val (term, nid) = attachDFOMonitor(node, id); id = nid
        super.apply(term)
      case node @ Defn.Def(_, Term.Name("main"), _, _ , _, _) =>
        super.apply(insertAtEndOfFunction(node, Constants.CONSOLIDATOR.parse[Stat].get))
      case Type.Name(name) if Constants.MAP_PRIM2SYM.contains(name) =>
        Type.Name(Constants.MAP_PRIM2SYM(name))
      case Term.If(predicate, ifBody, elseBody) =>
        val (term, nid) = attachPredicateMonitor(predicate, id); id = nid
        super.apply(Term.If(term, ifBody, elseBody))
      case node @ _ =>
        super.apply(node)
    }
  }
}




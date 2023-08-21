#!/bin/bash
# NOTE: RUN 'sbt assembly' FIRST IF YOU MADE CHANGES TO THE CODE
# NOTE: If you get scala.reflect.internal.MissingRequirementError, ensure that the machine is running java 8 when 'java' is invoked

# . Decide an output directory for coverage output
# . Run ScoverageInstrumenter.scala using the assembled jar file
# . Add scoverage-instrumented class files to jar using 'jar uf target/scala-2.11/$JAR_NAME target/scala-2.11/examples/fuzzable/<name of instrumented jars>'
# . Run a subject program e.g. examples.fuzzable.FlightDistance
# . Process the measurement files produced using CoverageMeasurementConsolidator.scala

# SAMPLE RUN:
#       ./run-fuzzer.sh WebpageSegmentation 20 seeds/reduced_data/webpage_segmentation/{before,after}

# Temporarily hard-coded, should be parsed from args
NAME=$1
DURATION=$2
shift 2
ARGS=$*

#CLASS_INSTRUMENTED=examples.fuzzable.$NAME # which class needs to be fuzzed DISC vs FWA
PATH_SCALA_SRC="src/main/scala/examples/fwa/$NAME.scala"
PATH_INSTRUMENTED_CLASSES="examples/fwa/$NAME*"
DIR_FUZZER_OUT="target/depfuzz-output/$NAME"
JAR_NAME=DepFuzz-assembly-1.0.jar

rm -rf $DIR_FUZZER_OUT
mkdir -p graphs $DIR_FUZZER_OUT/{scoverage-results,report,log,reproducers,crashes} || exit 1

sbt assembly || exit 1

java -cp  target/scala-2.12/$JAR_NAME \
          transformers.RunTransformer \
          $NAME \
          || exit

sbt assembly || exit 1

java -cp  target/scala-2.12/$JAR_NAME \
          utils.ScoverageInstrumenter \
          $PATH_SCALA_SRC \
          $DIR_FUZZER_OUT/scoverage-results/referenceProgram \
          || exit

pushd target/scala-2.12/classes || exit 1
jar uvf  ../$JAR_NAME \
        $PATH_INSTRUMENTED_CLASSES \
        || exit 1
popd || exit 1

java -cp  target/scala-2.12/$JAR_NAME \
          runners.RunFuzzerJar \
          $NAME \
          $DURATION \
          "$(realpath $DIR_FUZZER_OUT)" \
          $ARGS \
          || exit 1


python3 gen_graph.py \
        --coords-file $DIR_FUZZER_OUT/scoverage-results/referenceProgram/coverage.tuples \
        --outfile graphs/graph-$NAME-coverage.png \
        --title " Coverage: $NAME" \
        --x-label "Time (s)" \
        --y-label "Statement Coverage (%)" && echo "Graphs generated!"

#rm -rf src/main/scala/examples/{fwa,instrumented}
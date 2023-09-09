# Artifact: Co-Dependence Aware Fuzzing For Dataflow-based Big Data Analytics
## 1. Pre-requisites
This manual assumes `docker` is installed and set up properly on your device.\
These instructions have been tested with the following configurations: 
* **Ubuntu:** 20.04.6 LTS\
  **Docker:** 24.0.2, build cb74dfc
## 2. Creating the Docker Image
Clone this repository:
```
git clone https://github.com/SEED-VT/DepFuzz.git
```
Navigate into the repository folder:
```
cd DepFuzz
```
Build the docker image using:
> **_NOTE:_** If you receive a permission denied error you will need to prefix all docker commands with `sudo`. Wether or not this is needed depends on how docker is configured on your machine. A docker vulnerability warning at the end is okay and will not be an issue.
```
docker build . -t depfuzz --no-cache
```
> **_TROUBLESHOOTING:_** If the above command fails try restarting the docker service using: `sudo systemctl restart docker`

## 3. Running the Container
Make a directory to be used as a shared volume:
```
mkdir graphs
```
Obtain a shell instance inside the docker container:
```
docker run -v $(pwd)/graphs:/DepFuzz/graphs -it depfuzz bash
```
## 4. Running the Fuzzer
> **_NOTE:_** All following commands must be run on the shell instance inside the docker container

Navigate into the repository folder:
```
cd DepFuzz
```

We will show you how to run the fuzzer for one of the programs in the benchmark suite: `WebpageSegementation`. 
To fuzz `WebpageSegmentation` for 5 minutes, run:
```
./run-fuzzer.sh WebpageSegmentation 300 data/full_data/webpage_segmentation/{before,after}
```
> **_Expected Observation:_** The fuzzer will start and lots of output will be seen. Most of this is the output of the program that is being fuzzed. At the end of the 5 minutes, a Coverage vs Time graph will be generated as a `.png` file inside the `graphs` directory on your local machine (NOT the docker container). You may navigate to this directory and observe the graph. The final lines of the output should look something like this:
> ```
> == RESULTS: DepFuzz WebpageSegmentation ===
> # of Failures: 0 (0 unique)
> coverage progress: 38.79,70.3,76.97,80.61,86.67,87.88,88.48,94.55,95.76,96.36,96.97,97.58
> iterations: 1382
> Total Time (s): 300.51 (P: 0.1 | F: 300.406)
> Graphs generated!
> ```


The following is the general template of the command above, for running any of the benchmark programs:
```
./run-fuzzer.sh <PROGRAM_NAME> <DURATION> <DATASETPATH_1> ... <DATASETPATH_N>
```
`<PROGRAM_NAME>` must be replaced with the name of any scala file under `src/main/scala/examples/benchmark/` (the `.scala` extension must be omitted from the name). `<DURATION>` is the duration (in seconds) for the fuzzing campaign.

## 5. Reusability

The system is designed in a modular fashion, and the code follows good design practices. It should be easy to use, carve out and replace any component of the fuzzing system.
If one wants to inspect the codebase, they are recommended to start by reading `src/main/scala/runners/RunFuzzerJar.scala`. 

Here is some documentation of important packages explained w.r.t. the paper:

| Package    | Description |
| -------- | ------- |
| `abstraction`  | Contains code for framework abstraction    |
| `examples.benchmarks` |   Contains all the benchmark programs used in the paper   |
| `fuzzer`    | Contains the classes of the core fuzzer |
| `generators`    | Contains data generators for the programs |
| `guidance`    | Contains the main mutation co-dependent mutation code for DepFuzz |
| `monitoring`    | Contains the code for co-dependence monitors attached by DepFuzz |
| `runners`    | Starting point for executing the fuzzer |
| `scoverage`    | Contains the modified scoverage compiler extension to facilitate coverage capture |
| `sparkwrapper`    | Contains classes that are a wrapper around the Spark API required for Tainting |
| `taintedprimitives`    | Contains Tainted versions of primitive data types |
| `transformers`    | Contains the code for automated static re-write of benchmarks |
| `utils`    | Miscellaneous helper classes |

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
> **_NOTE:_** Depending on how docker is configured, you may need to `sudo` this
```
docker build . -t depfuzz --no-cache
```
> **_TROUBLESHOOTING:_** If the above command fails try restarting the docker service using: `sudo systemctl restart docker`

## 3. Running the Container
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
./run-fuzzer.sh WebpageSegmentation 300 data/webpage_segmentation/{before,after}
```
> **_Expected Observation:_** The fuzzer will start and lots of output will be seen. At the end of the 5 minutes, a Coverage vs Time graph will be generated as a `.png` file inside the `graphs` directory on your local machine (NOT the docker container). You may navigate to this directory and observe


The following is the general template of the command above, for running any of the benchmark programs:
```
./run-fuzzer.sh <PROGRAM_NAME> <DURATION> <DATASETPATH_1> ... <DATASETPATH_N>
```
`<PROGRAM_NAME>` must be replaced with the name of any scala file under `src/main/scala/examples/benchmark/` (the `.scala` extension must be omitted from the name)

## 5. Observing the Output

Head over to the `graphs` directory in the repository cloned on your device. You should see a Coverage vs Time graph for WebpageSegmentation.


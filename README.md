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
docker run -v ./graphs:/DepFuzz/graphs -it depfuzz bash
```
## 4. Running the Fuzzer
> **_NOTE:_** All following commands must be run on the shell instance inside the docker container

Navigate into the repository folder:
```
cd DepFuzz
```
The following is the template command for running any of the benchmark programs:
```
./run-fuzzer.sh <PROGRAM_NAME> <DURATION> <DATASETPATH_1> ... <DATASETPATH_N>
```
For `PROGRAM_NAME` you may pass in the name of any scala file under `src/main/scala/examples/benchmark/` after omitting the extension

We will show you how to run the tool for WebpageSegementation. To fuzz `WebpageSegmentation` for 5 minutes, run:
```
./run-fuzzer.sh WebpageSegmentation 300 data/webpage_segmentation/{before,after}
```


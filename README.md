# Artifact: Co-Dependence Aware Fuzzing For Dataflow-based Big Data Analytics
## 1. Pre-requisites
This manual assumes `docker` is installed and set up properly on your device.\
These instructions have been tested with the following configurations: 
* **Ubuntu:** 20.04.6 LTS\
  **Docker:** 24.0.2, build cb74dfc
## 2. Creating the Docker Image
Clone this repository:
```
git clone https://github.com/ahmayun/naturalfuzz.git
```
Navigate into the repository folder:
```
cd NaturalFuzz
```
Build the docker image using:
> **_NOTE:_** Depending on how docker is configured, you may need to `sudo` this
```
docker build . -t naturalfuzz --no-cache
```
> **_TROUBLESHOOTING:_** If the above command fails try restarting the docker service using: `sudo systemctl restart docker`

## 3. Running the Container
Obtain a shell instance inside the docker container:
```
docker run -v ./graphs:/NaturalFuzz/graphs -it naturalfuzz bash
```
## 4. Running the Experiments
> **_NOTE:_** All following commands must be run on the shell instance inside the docker container

Navigate into the repository folder:
```
cd NaturalFuzz
```
The following is the template command for running any of the benchmark programs:
```
./run-fuzzer.sh <PROGRAM_NAME> <DURATION> <DATASETPATH_1> ... <DATASETPATH_N>
```
For `PROGRAM_NAME` you may pass in the name of any scala file under `src/main/scala/examples/tpcds/` after omitting the extension

We will show you how to run the tool for Q1. To fuzz `Q1` for 5 minutes, run:
```
./run-fuzzer.sh Q1 300 data/{store_returns,data_dim,store,customer}
```


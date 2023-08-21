FROM amazoncorretto:8u372-al2

ENV REPO_LINK=https://github.com/SEED-VT/DepFuzz.git
ENV REPO_NAME=DepFuzz

RUN yum -y update
RUN yum install -y unzip
RUN yum install -y zip
RUN yum install -y gzip
RUN curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs 
RUN chmod +x cs 
RUN ./cs setup -y

ENV PATH="$PATH:/root/.local/share/coursier/bin"

RUN yum install -y git
RUN yum install -y python3
RUN python3 -m pip install pandas seaborn matplotlib
RUN git clone $REPO_LINK
RUN mkdir -p $REPO_NAME/graphs
RUN cd $REPO_NAME && sbt assembly

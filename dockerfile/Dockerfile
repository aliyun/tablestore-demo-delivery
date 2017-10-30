FROM fedora:latest
RUN yum upgrade -y
RUN yum install -y python2 unzip wget scons golang git
RUN go get github.com/aliyun/aliyun-tablestore-go-sdk/tablestore
WORKDIR /opt/demo

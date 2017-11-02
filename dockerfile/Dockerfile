FROM fedora:latest
RUN yum upgrade -y
RUN yum install -y python2 unzip wget scons golang git
RUN go get github.com/aliyun/aliyun-tablestore-go-sdk/tablestore
RUN wget https://gosspublic.alicdn.com/fcli/fcli-v0.11-linux-amd64.zip && unzip fcli-v0.11-linux-amd64.zip
WORKDIR /opt/demo

FROM ubuntu
MAINTAINER zhouwench@gmail.com

env WORKDIR /home/ubuntu/work
RUN mkdir -p $WORKDIR
COPY binstore $WORKDIR/binstore
COPY binstore_bin $WORKDIR/binstore/bin/binstore
COPY binstore.yaml $WORKDIR/binstore/conf/binstore.yaml

VOLUME ["$WORKDIR/binstore/log"]

CMD ["/home/ubuntu/work/binstore/op.sh", "start"]

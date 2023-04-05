FROM openjdk:8-jre-alpine

RUN  apk add python3 && \
    apk upgrade musl

# install miniconda and pyincore
RUN apk --update add curl wget ca-certificates libstdc++ glib && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://raw.githubusercontent.com/sgerrand/alpine-pkg-node-bower/master/sgerrand.rsa.pub && \
    curl -L "https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.23-r3/glibc-2.23-r3.apk" -o glibc.apk && \
    apk add glibc.apk && \
    curl -L "https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.23-r3/glibc-bin-2.23-r3.apk" -o glibc-bin.apk && \
    apk add glibc-bin.apk && \
    curl -L "https://github.com/andyshinn/alpine-pkg-glibc/releases/download/2.25-r0/glibc-i18n-2.25-r0.apk" -o glibc-i18n.apk && \
    apk add --allow-untrusted glibc-i18n.apk && \
    /usr/glibc-compat/bin/localedef -i en_US -f UTF-8 en_US.UTF-8 && \
    /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc/usr/lib && \
    rm -rf glibc*apk /var/cache/apk/* && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-py38_4.8.3-Linux-x86_64.sh -O miniconda.sh && \
    sh miniconda.sh -b

ENV PATH="/root/miniconda3/condabin:$PATH"
# install pyincore
WORKDIR /app
COPY run_pyincore.sh dw_pyincore.py pyincore-env.yaml input_definition.json /app
RUN conda env create -f /app/pyincore-env.yaml && \
  conda clean -a -y

# Entrypoint - this will activate conda and run dw_pyincore.py with given arguments
ENTRYPOINT ["/app/run_pyincore.sh"]

FROM apache/spark:3.5.8-python3

USER root

RUN pip3 install --no-cache-dir \
    pandas \
    pyarrow \
    numpy \
    psutil

USER spark

WORKDIR /app
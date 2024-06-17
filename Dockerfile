FROM python:3.12-bookworm

RUN apt-get -qq update && apt-get -qq -y upgrade
RUN apt-get install -qq -y pkg-config libicu-dev libleveldb-dev
RUN apt-get -qq -y autoremove && apt-get clean

RUN pip install --no-cache-dir -q -U pip setuptools
RUN pip install --no-cache-dir -q --no-binary=:pyicu: pyicu

COPY ftmq /src/ftmq
COPY setup.py /src/setup.py
COPY README.md /src/README.md
COPY pyproject.toml /src/pyproject.toml
COPY VERSION /src/VERSION

WORKDIR /src
RUN pip install --no-cache-dir -U pip setuptools
RUN pip install --no-cache-dir ".[redis,clickhouse,level]"

ENTRYPOINT ["ftmq"]

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget build-essential git \
    bash coreutils findutils grep sed gawk \
    tzdata gzip \
    inotify-tools \
    gosu \
 && rm -rf /var/lib/apt/lists/*

# Build RTKLIBExplorer v2.5.0 (str2str + convbin)
RUN wget -qO - https://github.com/rtklibexplorer/RTKLIB/archive/refs/tags/v2.5.0.tar.gz | tar -xz \
 && cd RTKLIB-2.5.0 \
 && make --directory=app/consapp/str2str/gcc \
 && make --directory=app/consapp/str2str/gcc install \
 && make --directory=app/consapp/convbin/gcc \
 && make --directory=app/consapp/convbin/gcc install \
 && cd / \
 && rm -rf RTKLIB-2.5.0

# Build RNXCMP (Hatanaka) tools: rnx2crx / crx2rnx (GSI)
RUN mkdir -p /tmp/rnxcmp \
 && wget -qO - https://terras.gsi.go.jp/ja/crx2rnx/RNXCMP_4.2.0_src.tar.gz | tar -xz -C /tmp/rnxcmp \
 && cd /tmp/rnxcmp/RNXCMP_4.2.0_src/source \
 && cc -O2 -o /usr/local/bin/rnx2crx rnx2crx.c \
 && cc -O2 -o /usr/local/bin/crx2rnx crx2rnx.c \
 && rm -rf /tmp/rnxcmp

COPY scripts/ /opt/scripts/
RUN chmod +x /opt/scripts/*.sh

ENTRYPOINT ["/opt/scripts/entrypoint.sh"]


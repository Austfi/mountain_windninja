FROM ubuntu:22.04

SHELL ["/bin/bash", "-c"]

ARG WINDNINJA_REF=3.12.2

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=UTC \
    WM_PROJECT_INST_DIR=/opt \
    VIRTUAL_ENV=/opt/venv \
    OMPI_ALLOW_RUN_AS_ROOT=1 \
    OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 \
    PATH=/opt/venv/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin \
    LD_LIBRARY_PATH=/usr/local/lib \
    MWN_PYTHON_BIN=/opt/venv/bin/python \
    MWN_WINDNINJA_CLI=/usr/local/bin/WindNinja_cli \
    MWN_OPENFOAM_BASHRC=/opt/openfoam9/etc/bashrc

WORKDIR /opt/mountain_windninja

COPY docker/patches/windninja-public-pastcast.patch /tmp/windninja-public-pastcast.patch

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      apt-transport-https \
      build-essential \
      ca-certificates \
      cmake \
      cron \
      file \
      gfortran \
      git \
      gnupg2 \
      libboost-date-time-dev \
      libboost-program-options-dev \
      libboost-test-dev \
      libcurl4-gnutls-dev \
      libfontconfig1-dev \
      libgeos-dev \
      libnetcdf-dev \
      libopenjp2-7-dev \
      libsqlite3-dev \
      libtiff-dev \
      pkg-config \
      python3 \
      python3-pip \
      python3-venv \
      sqlite3 \
      software-properties-common \
      sudo \
      wget && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    git clone --branch "${WINDNINJA_REF}" --depth 1 \
        https://github.com/firelab/windninja.git /opt/src/windninja && \
    git -C /opt/src/windninja apply /tmp/windninja-public-pastcast.patch && \
    cd /opt/src && \
    wget https://poppler.freedesktop.org/poppler-22.02.0.tar.xz && \
    tar -xf poppler-22.02.0.tar.xz && \
    cd /opt/src/poppler-22.02.0 && \
    mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_INSTALL_PREFIX=/usr/local \
          -DENABLE_UNSTABLE_API_ABI_HEADERS=ON \
          .. && \
    make -j"$(nproc)" && \
    make install && \
    cd /opt/src && \
    rm -rf /opt/src/poppler-22.02.0 /opt/src/poppler-22.02.0.tar.xz && \
    wget https://download.osgeo.org/proj/proj-8.2.1.tar.gz && \
    tar -xf proj-8.2.1.tar.gz && \
    cd /opt/src/proj-8.2.1 && \
    ./configure --prefix=/usr/local && \
    make clean && \
    make -j"$(nproc)" && \
    make install && \
    cd /opt/src && \
    rm -rf /opt/src/proj-8.2.1 /opt/src/proj-8.2.1.tar.gz && \
    wget https://download.osgeo.org/gdal/3.4.1/gdal-3.4.1.tar.gz && \
    tar -xf gdal-3.4.1.tar.gz && \
    cd /opt/src/gdal-3.4.1 && \
    ./configure --prefix=/usr/local --with-poppler=/usr/local && \
    make -j"$(nproc)" && \
    make install && \
    cd /opt/src && \
    rm -rf /opt/src/gdal-3.4.1 /opt/src/gdal-3.4.1.tar.gz && \
    wget -O /etc/apt/trusted.gpg.d/openfoam.asc https://dl.openfoam.org/gpg.key && \
    add-apt-repository -y http://dl.openfoam.org/ubuntu && \
    apt-get update && \
    apt-get install -y --no-install-recommends openfoam9 && \
    printf 'source /opt/openfoam9/etc/bashrc\n' >> /etc/bash.bashrc && \
    ldconfig && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/src/windninja/build && \
    cd /opt/src/windninja/build && \
    cmake \
      -D SUPRESS_WARNINGS=ON \
      -D NINJAFOAM=ON \
      -D BUILD_FETCH_DEM=ON \
      -D BUILD_SLOPE_ASPECT_GRID=ON \
      -D BUILD_FLOW_SEPARATION_GRID=ON \
      -D NINJA_GUI=OFF \
      -D NINJA_QTGUI=OFF \
      .. && \
    make -j"$(nproc)" && \
    make install && \
    ldconfig

RUN source /opt/openfoam9/etc/bashrc && \
    mkdir -p "$FOAM_RUN/../applications" && \
    cp -r /opt/src/windninja/src/ninjafoam/9/* "$FOAM_RUN/../applications" && \
    cd "$FOAM_RUN/../applications" && \
    wmake libso && \
    cd utility/applyInit && \
    wmake && \
    cp "$FOAM_RUN/../platforms/linux64GccDPInt32Opt/lib/libWindNinja.so" \
       /opt/openfoam9/platforms/linux64GccDPInt32Opt/lib/ && \
    cp "$FOAM_RUN/../platforms/linux64GccDPInt32Opt/bin/applyInit" \
       /opt/openfoam9/platforms/linux64GccDPInt32Opt/bin/ && \
    chmod 644 /opt/openfoam9/platforms/linux64GccDPInt32Opt/lib/libWindNinja.so && \
    chmod 755 /opt/openfoam9/platforms/linux64GccDPInt32Opt/bin/applyInit

COPY requirements.txt /opt/mountain_windninja/requirements.txt

RUN python3 -m venv "$VIRTUAL_ENV" && \
    "$VIRTUAL_ENV/bin/pip" install --upgrade pip && \
    "$VIRTUAL_ENV/bin/pip" install --no-cache-dir -r /opt/mountain_windninja/requirements.txt

COPY . /opt/mountain_windninja

RUN chmod +x \
    /opt/mountain_windninja/docker/start_scheduler.sh \
    /opt/mountain_windninja/deploy/gcp/install_docker_host.sh \
    /opt/mountain_windninja/deploy/gcp/mwn.sh \
    /opt/mountain_windninja/scripts/run_cron.sh \
    /opt/mountain_windninja/scripts/run_windninja.sh \
    /opt/mountain_windninja/scripts/setup_cron.sh

RUN mkdir -p /opt/mountain_windninja/runtime/logs /opt/mountain_windninja/static_data

CMD ["/bin/bash"]

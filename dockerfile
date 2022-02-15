FROM ubuntu:18.04

# make bash the default shell
SHELL [ "/bin/bash", "--login", "-c" ]

RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y wget

# get graphics
RUN apt install libgl1-mesa-glx -y
RUN apt-get install ffmpeg libsm6 libxext6  -y

# Create a non-root user
ARG username=eyou102
ARG uid=1000
ARG gid=100
ENV USER $username
ENV UID $uid
ENV GID $gid
ENV HOME /home/$USER
RUN adduser --disabled-password \
    --gecos "Non-root user" \
    --uid $UID \
    --gid $GID \
    --home $HOME \
    $USER

USER $USER
# install miniconda
ENV MINICONDA_VERSION 4.10.3
ENV CONDA_DIR $HOME/miniconda3
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-py38_$MINICONDA_VERSION-Linux-x86_64.sh -O ~/miniconda.sh && \
    chmod +x ~/miniconda.sh && \
    ~/miniconda.sh -b -p $CONDA_DIR && \
    rm ~/miniconda.sh
# make non-activate conda commands available
ENV PATH=$CONDA_DIR/bin:$PATH
# make conda activate command available from /bin/bash --login shells
RUN echo ". $CONDA_DIR/etc/profile.d/conda.sh" >> ~/.profile
# make conda activate command available from /bin/bash --interative shells
RUN conda init bash

# create a project directory inside user home
ENV PROJECT_DIR $HOME/kluster
RUN mkdir $PROJECT_DIR
WORKDIR $PROJECT_DIR

# build the conda environment
RUN conda update --name base --channel defaults conda
RUN conda create -n kluster_test python=3.8.12

# Make RUN commands use the new environment, activate doesnt work:
# https://pythonspeed.com/articles/activate-conda-dockerfile/
SHELL ["conda", "run", "-n", "kluster_test", "/bin/bash", "-c"]

RUN conda install -c conda-forge qgis=3.18.3 vispy=0.9.4 pyside2=5.13.2 gdal=3.3.1 h5py python-geohash
# conda run -n kluster_test pip install git+https://github.com/noaa-ocs-hydrography/kluster.git#egg=hstb.kluster
# conda run -n kluster_test python -m HSTB.kluster


FROM ubuntu:18.04

# SNAP is still stuck with Python 3.6, i.e. ubuntu:18.04
# https://forum.step.esa.int/t/modulenotfounderror-no-module-named-jpyutil/25785/2

LABEL version="1.0"

ENV DEBIAN_FRONTEND noninteractive

USER root

WORKDIR /root/sentinel_process
COPY . .

# Install dependencies and tools
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends --no-install-suggests \
    build-essential \
    libgfortran5 \
    locales \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools \
    git \
    vim \
    wget \
    zip \
    && apt-get autoremove -y \
    && apt-get clean -y \ 
    apt-get install -y awscli

# Set the locale
ENV LANG en_US.utf8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.utf8




# SNAP wants the current folder '.' included in LD_LIBRARY_PATH
ENV LD_LIBRARY_PATH ".:$LD_LIBRARY_PATH"

# install SNAPPY
# RUN apt-get update && apt-get upgrade -y
RUN apt-get update && apt-get install default-jdk maven -y
# RUN apt-get install -qy default-jdk && \
    # apt-get install -qy maven
ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64/"
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1
COPY snap /src/snap
RUN bash /src/snap/install.sh
RUN update-alternatives --remove python /usr/bin/python3

# due to Ubuntu-GDAL being too old we prefer to use the SNAP-bundled GDAL:
# INFO: org.esa.s2tbx.dataio.gdal.GDALVersion: GDAL not found on system. Internal GDAL 3.0.0 from distribution will be used. (f1)

# path
RUN echo "export PATH=\$PATH:/usr/local/snap/bin/:/root/.snap/auxdata/gdal/gdal-3-2-1/bin" >> /root/.bashrc

# tests
# https://senbox.atlassian.net/wiki/spaces/SNAP/pages/50855941/Configure+Python+to+use+the+SNAP-Python+snappy+interface
RUN (cd /root/.snap/snap-python/snappy && python3 setup.py install)
RUN /usr/bin/python3 -c 'from snappy import ProductIO'
RUN /usr/bin/python3 /src/snap/about.py
RUN /root/.snap/auxdata/gdal/gdal-3-2-1/bin/gdal-config --version

# When using SNAP from Python, either do: sys.path.append('/root/.snap/snap-python')

# Reduce the image size
RUN apt-get autoremove -y
RUN apt-get clean -y
RUN rm -rf /src

# RUN APP
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade -r /requirements.txt
RUN pip install requests==2.7.0

# ARG AWS_SECURITY_TOKEN
# Set docker basics
VOLUME /usr/app
# ARG MYSQL_SECRET='mysql_secret'

RUN apt-get update -y
RUN apt-get install libpq-dev -y
RUN apt-get install default-libmysqlclient-dev  -y
RUN apt-get install pkg-config -y

# RUN python -m pip install boto3

# COPY ./test.py /usr/app/test.py
# RUN python /usr/app/test.py


# ENTRYPOINT ["/bin/bash"]
# #CMD ["/src/start.sh"]

# Use /bin/bash as the default shell
SHELL ["/bin/bash", "-c"]

# Run the Python script using python3
# CMD ["python3", "/root/sentinel_process/workspace/clone_process.py", "-d", "10", "-m", "2", "-y", "2024", "-f", "MPK", "-or", "S1_ASC", "-p", "S1A_IW_GRDH_1SDV_20240210T112151_20240210T112216_052496_065965_D1FB"]
# CMD ["python3", "/root/sentinel_process/workspace/process.py"]
CMD ["python3", "/root/sentinel_process/workspace/clone_process.py", "-d", "21", "-m", "1", "-y", "2024", "-f", "MPK", "-or", "S1_DES", "-p", "S1A_IW_GRDH_1SDV_20240121T230050_20240121T230115_052211_064FCE_DE4F"]

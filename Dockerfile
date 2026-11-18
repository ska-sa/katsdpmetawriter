ARG KATSDPDOCKERBASE_REGISTRY=harbor.sdp.kat.ac.za/dpp

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-build AS build

# uv is installed in ~/.local/bin in the base image
# Activate the base image virtualenv and make uv available
ENV PATH="/home/kat/.local/bin:$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"


# Install dependencies
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt

RUN uv pip compile /tmp/install/requirements.txt \
        -c /home/kat/docker-base/base-requirements.lock \
        -o /tmp/install/requirements.lock && \
    uv pip install --no-deps -r /tmp/install/requirements.lock && \
    uv pip check

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpmetawriter
WORKDIR /tmp/install/katsdpmetawriter

# Required to build pycairo - provides cairo header files and cairo.pc for pkg-config
USER root
RUN apt-get update && apt-get install -y \
    libcairo2-dev \
    && apt-get clean
USER kat

RUN pip install "setuptools<70" "wheel" pycairo&& \
    uv pip install --no-deps . && \
    pip check

#######################################################################

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-runtime
LABEL maintainer="sdpdev+katsdpmetawriter@ska.ac.za"

COPY --from=build --chown=kat:kat /home/kat/ve3 /home/kat/ve3
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

EXPOSE 2049

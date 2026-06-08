ARG KATSDPDOCKERBASE_REGISTRY=harbor.sdp.kat.ac.za/dpp


FROM $KATSDPDOCKERBASE_REGISTRY/base-build:focaluvpip AS build

# Switch to Python 3 environment
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Install dependencies
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt
#RUN install_pinned.py -r /tmp/install/requirements.txt
RUN uv pip compile /tmp/install/requirements.txt \
    -o /tmp/install/requirements.lock && \
    uv pip sync /tmp/install/requirements.lock --strict

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpmetawriter
WORKDIR /tmp/install/katsdpmetawriter
#RUN python ./setup.py clean
#RUN pip install --no-deps .
#RUN pip check
RUN pip install "setuptools<70" "wheel" && \
    uv pip install --no-deps . && \
    pip check

#######################################################################

FROM $KATSDPDOCKERBASE_REGISTRY/base-runtime:focaluvpip
LABEL maintainer="sdpdev+katsdpmetawriter@ska.ac.za"

COPY --from=build --chown=kat:kat /home/kat/ve3 /home/kat/ve3
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

EXPOSE 2049

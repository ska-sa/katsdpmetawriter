ARG KATSDPDOCKERBASE_REGISTRY=quay.io/ska-sa

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-build as build

# Switch to Python 3 environment
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Install dependencies
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt
RUN install_pinned.py -r /tmp/install/requirements.txt

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpmetawriter
WORKDIR /tmp/install/katsdpmetawriter
RUN python ./setup.py clean
RUN pip install --no-deps .
RUN pip check

#######################################################################

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-runtime
LABEL maintainer="sdpdev+katsdpmetawriter@ska.ac.za"

COPY --from=build --chown=kat:kat /home/kat/ve3 /home/kat/ve3
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

EXPOSE 2049

FROM sdp-docker-registry.kat.ac.za:5000/docker-base

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

# Switch to Python 3 environment
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Install dependencies
COPY requirements.txt /tmp/install/requirements.txt
RUN install-requirements.py -d ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpmetawriter
WORKDIR /tmp/install/katsdpmetawriter
RUN python ./setup.py clean && pip install --no-deps . && pip check

EXPOSE 2049

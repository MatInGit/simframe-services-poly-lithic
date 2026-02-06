FROM isisacceleratorcontrols/poly-lithic:base-v1.7.3

WORKDIR /app

# Copy the plugin package
COPY . /app/simframe_services

# Install the plugin
RUN pip install -e /app/simframe_services
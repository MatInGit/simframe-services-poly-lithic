FROM isisacceleratorcontrols/poly-lithic:base-v0.1.7.2

WORKDIR /app

# Copy the plugin package
COPY . /app/simframe_services

# Install the plugin
RUN pip install -e /app/simframe_services
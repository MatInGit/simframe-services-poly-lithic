FROM isisacceleratorcontrols/poly-lithic:base-v0.1.7.2

WORKDIR /app

# Copy the plugin package
COPY . /app

# Install the plugin
RUN pip install -e /app
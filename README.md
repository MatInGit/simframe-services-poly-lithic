# How to run the test setup

```bash
docker compose -f docker_compose.yaml up --build
```
Note that the kafka broker is assumed to be running at athena.isis.rl.ac.uk:9092. Adjust the configuration in the command args in `docker_compose.yaml` if needed.

Testing:

```bash
python test_wrangler_client.py
``` 
<!-- 
# simframe_services

A poly_lithic plugin package

## Installation

```bash
pip install -e .
```
or 
```bash
uv pip install -e .
```

## Quick Start

Test your plugin with the included deployment configuration:

```bash
# Run the test deployment
pl run --config test_deployment.yaml --debug

# Or test with one-shot mode (runs once and exits)
pl run --config test_deployment.yaml --debug --one-shot
```

**Note:** Edit `test_deployment.yaml` and comment out the modules for plugin types you don't need.

## Usage

This package provides three types of plugins. Comment out the ones you don't need:

### Interface Plugin
```python
from poly_lithic.src.utils.plugin_registry import interface_plugin_registry

interface = interface_plugin_registry.get("simframe_services_interface")
instance = interface(config)
```

### Transformer Plugin
```python
from poly_lithic.src.utils.plugin_registry import transformer_plugin_registry

transformer = transformer_plugin_registry.get("simframe_services_transformer")
instance = transformer(config)
```

### Model Getter Plugin
```python
from poly_lithic.src.utils.plugin_registry import model_getter_plugin_registry

getter = model_getter_plugin_registry.get("simframe_services_model_getter")
model = getter.load_model()
```

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

## Customization

1. Edit `simframe_services/plugins.py` to implement your plugin logic
2. Comment out plugin types you don't need in `simframe_services/__init__.py`
3. Update `pyproject.toml` entry points to match your needs

## License

simframe_services is released under the MIT License. -->
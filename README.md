# How to run the test setup

```bash
docker compose -f docker_compose.yaml up --build
```
Adjust the configuration in the command args in `docker_compose.yaml` if needed.

## Installation

In your environment which already has poly_lithic installed, run:
```bash
pip install -e .

pl plugins list
```
You should see `k2simframe...` plugins in the list.

![alt text](image.png)

Testing:

```bash
python test_wrangler_client.py # this needs connection to kafka
``` 

## Example Usage
```
docker compose up
```
open http://localhost:8012/docs

List models:
```
curl -X 'GET' \
  'http://localhost:8012/list_models/' \
  -H 'accept: application/json'
```

Submit lattice to `test-model-1`:
```

curl -X 'POST' \
  'http://localhost:8012/submit_lattice/test-model-1' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "model": "string",
  "beam": {},
  "lattice_name": "string",
  "lattice": {},
  "job_id": "test-0"
}'
```


Get result for job `test-0`:

```
Curl

curl -X 'GET' \
  'http://localhost:8012/get_result/test-0' \
  -H 'accept: application/json'
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
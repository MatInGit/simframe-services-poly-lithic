"""
The wrangler module is a frontend for multiple poly-lithic models.

Sequence of events is as follows:

The wrangler has two channels of communication:

- RestAPI for synchronous calls between simframe and the wrangler
- Kafka for asynchronous messaging between simframe and the wrangler

SimFrame makes a call to the wrangler to request model settings         - RestAPI
Simframe builds a lattice according to the model settings.              - intetrnal to simframe, not our concern
Simframe sends the lattice to the wrangler for processing.              - RestAPI
Wrangeler polls the model for results and gets its once ready           - RestAPI
# Simframe emits a messgage with the job id to the wrangler               - Kafka this step is unnecessary as we are implicitly sending kafka message when we are ready 
Wrangler sends emits a kafka message with with the job id               - Kafka
SimFrmae picks up the message and fetches the results from the wrangler - RestAPI

"""

from fastapi import FastAPI, HTTPException, Request
from fastapi import status as http_status
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional

from confluent_kafka import Producer, KafkaError, KafkaException ,Consumer
from confluent_kafka.admin import AdminClient, NewTopic

import uvicorn
import click
import requests
import asyncio
import json
import time
import httpx

from simframe_services.schemas import ModelRegistration, LatticeJob

# server calass to store data and states

class WranglerServer:
    def __init__(self, kafka_broker: str):
        self.kafka_broker = kafka_broker
        self.producer_conf = {'bootstrap.servers': kafka_broker}
        self.consumer_conf = {
            'bootstrap.servers': kafka_broker,
            'group.id': 'wrangler_group',
            'auto.offset.reset': 'earliest'
        }
        self.producer = Producer(self.producer_conf)
        
        self.registered_models: Dict[str, ModelRegistration] = {}
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.results: Dict[str, Any] = {}
        
        
        self.producer_conf = {'bootstrap.servers': kafka_broker}
        self.producer = Producer(self.producer_conf)
        
        self.consumer_conf = {
            'bootstrap.servers': kafka_broker,
            'group.id': 'wrangler_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_conf)
        
        self.kakfa_job_ids = [] # list of job ids received via kafka, this means someone is waiting for results
        
    def __kafka_monitor_process(self):
        # monitor kafka for incoming messages and respond with results to corresponding jobids
        # This is a SYNCHRONOUS blocking function that runs in a thread
        
        self.consumer.subscribe(['simframe_requests'])
        
        while True:
            # go though the queue of job ids and check if we have results for them
            for job_id in list(self.kakfa_job_ids):  # Use list() to avoid modification during iteration
                if job_id in self.results:
                    # we have results for this job
                    result_message = {
                        "job_id": job_id,
                        "status": "completed",
                    }
                    self.producer.produce("wrangler_results", key=job_id, value=json.dumps(result_message))
                    print(f"Emitted results for job {job_id} to kafka.")
                    self.producer.flush()
                    self.kakfa_job_ids.remove(job_id)
            
            
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    continue
            try:
                message_value = msg.value().decode('utf-8')
                message = json.loads(message_value)
                print(f"Received Kafka message: {message}")
                
                job_id = message.get("job_id")
                
                if job_id in self.results:
                    # we have results for this job
                    result_message = {
                        "job_id": job_id,
                        "status": "completed",
                    }
                    self.producer.produce("wrangler_results", key=job_id, value=json.dumps(result_message))
                    print(f"Emitted results for job {job_id} to kafka.")
                    self.producer.flush()
                else:
                    self.kakfa_job_ids.append(job_id)
                    print(f"No results found for job {job_id}, added to queue")
            except Exception as e:
                print(f"Error processing Kafka message: {e}")
            
            time.sleep(0.1)  # Use time.sleep instead of asyncio.sleep since this is sync
        
    def start_kafka_monitor(self):
        # Start the kafka monitor in a separate thread
        import threading
        self.kafka_thread = threading.Thread(target=self.__kafka_monitor_process, daemon=True)
        self.kafka_thread.start()
        print("Kafka monitor thread started")
        
    def stop_kafka_monitor(self):
        self.consumer.close()
        self.producer.flush()

        



# click command group
@click.group()
def cli():
    """CLI for the wrangler service."""
    pass

@cli.command("run")
@click.option("--host", default="localhost", help="Host for the FastAPI server")
@click.option("--port", default=8000, help="Port for the FastAPI server")
@click.option("--kafka-broker", default="athena.isis.rl.ac.uk:9092", help="Kafka brokers")

def run_server(host: str = "0.0.0.0", port: int = 1001, kafka_broker: str = "localhost:9092"):
    """Run the wrangler FastAPI server."""
    app = FastAPI(title="Wrangler Service", version="1.0.0")
    
    server = WranglerServer(kafka_broker)
    server.start_kafka_monitor()
    

    @app.post("/register_model", status_code=http_status.HTTP_201_CREATED)
    async def register_model(model: ModelRegistration):
        """Register a new model."""
        
        if model.model_id in server.registered_models:
            raise HTTPException(status_code=http_status.HTTP_400_BAD_REQUEST, detail="Model ID already exists.")
        server.registered_models[model.model_id] = model
        return {"message": "Model registered successfully.", "model": model}
    
    @app.delete("/delete_model/{model_id}", status_code=http_status.HTTP_200_OK)
    async def delete_model(model_id: str):
        """Delete a registered model."""
        if model_id not in server.registered_models:
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found.")
        del server.registered_models[model_id]
        return {"message": "Model deleted successfully."}
    
    @app.get("/models/{model_id}", response_model=ModelRegistration)
    async def get_model(model_id: str):
        """Get model registration information."""
        model = server.registered_models.get(model_id)
        if not model:
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found.")
        return model
    

    @app.get("/list_models/", response_model=Dict[str, ModelRegistration])
    async def list_models():
        return server.registered_models
    
    
    
    # proxied endpoints
    
    @app.get("/get_settings/{model_id}", response_class=JSONResponse)
    async def get_settings(model_id: str):
        # forwards the request to the model to get its settings
        if model_id not in server.registered_models:
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found.")
        model = server.registered_models[model_id]
        
        model_settings = requests.get(f"{model.api_url}/get_settings")
        
        if model_settings.status_code != 200:
            raise HTTPException(status_code=http_status.HTTP_502_BAD_GATEWAY, detail="Failed to get settings from model.")
        return JSONResponse(content=model_settings.json())
    
    @app.post("/submit_lattice/{model_id}", response_class=JSONResponse)
    async def submit_lattice(model_id: str, lattice_job: LatticeJob):
        # forwards the lattice to the model for processing
        if model_id not in server.registered_models:
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Model not found.")
        model = server.registered_models[model_id]
        
        response = requests.post(f"{model.api_url}/submit_lattice", json=lattice_job.model_dump())
        
        if response.status_code != 202:
            raise HTTPException(status_code=http_status.HTTP_502_BAD_GATEWAY, detail="Failed to submit lattice to model.")
        
        # start the async polling for results
        
        asyncio.create_task(_poll_model_for_results(model, response.json().get("job_id")))
        
        return {"message": "Lattice submitted successfully.", "response": response.json()}
    
    @app.get("/list_results/", response_class=JSONResponse)
    async def list_results():
        # list all results
        return JSONResponse(content=server.results)

    @app.get("/get_result/{job_id}", response_class=JSONResponse)
    async def get_result(job_id: str):
        # get results for a specific job
        if job_id not in server.results:
            #check if job is still processing
            if job_id in server.jobs:
                return JSONResponse(content={"message": "Job is still processing."}, status_code=http_status.HTTP_202_ACCEPTED)
            else:
                raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Job not found.")    
        
        return JSONResponse(content=server.results[job_id])
    
    async def _poll_model_for_results(model: ModelRegistration, job_id: str):
        # poll the model for results
        
        # start time 
        start = time.time() # we will timeout after 60 seconds
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    server.jobs[job_id] = {
                        "model_id": model.model_id,
                        "timestamp": time.time(),
                        "status": "processing"
                    }
                    response = await client.get(f"{model.api_url}/get_result", params={"job_id": job_id})
                    print(f"Polling {model.api_url}/get_result?job_id={job_id} - Status: {response.status_code}")
                    if response.status_code == 200:
                        # # got results
                        # results = response.json()
                        # # emit kafka message
                        # message = {
                        #     "model_id": model.model_id,
                        #     "job_id": job_id,
                        #     "results": results
                        # }
                        # server.producer.produce("wrangler_results", key=job_id, value=json.dumps(message))
                        # print(f"Emitted results for job {job_id} to kafka.")
                        # server.producer.flush()
                        
                        # save results
                        server.results[job_id] = response.json()
                        print(f"Saved results for job {job_id}.")
                        # clear job result from model
                        await client.delete(f"{model.api_url}/clear_result", params={"job_id": job_id})
                        print(f"Cleared results for job {job_id} from model {model.model_id}.")
                        
                        server.jobs[job_id]["status"] = "completed"
                        
                        break
                except Exception as e:
                    print(f"Error polling for results: {e}")
                
                # wait and poll again
                await asyncio.sleep(1)
                if time.time() - start > 60:
                    # timeout after 60 seconds
                    print(f"Timeout polling for results for job {job_id}")
                    break
    
    
    uvicorn.run(app, host=host, port=port)
    
    
    
if __name__ == "__main__":
    cli()
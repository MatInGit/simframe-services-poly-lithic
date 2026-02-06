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

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any

from confluent_kafka import Producer, KafkaError, Consumer

import uvicorn
import click
import json
import time

from simframe_services.schemas import ModelRegistration
from simframe_services.routes.wrangler import setup_v1_routes, setup_v2_routes

# server calass to store data and states


class WranglerServer:
    def __init__(self, kafka_broker: str, kafka_enabled: bool = True):
        self.kafka_broker = kafka_broker
        self.kafka_enabled = kafka_enabled

        self.registered_models: Dict[str, ModelRegistration] = {}
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.results: Dict[str, Any] = {}
        self.kakfa_job_ids = []  # list of job ids received via kafka, to be sent 

        # Only initialize Kafka if enabled
        if self.kafka_enabled:
            self.producer_conf = {"bootstrap.servers": kafka_broker}
            self.consumer_conf = {
                "bootstrap.servers": kafka_broker,
                "group.id": "wrangler_group",
                "auto.offset.reset": "earliest",
            }
            self.producer = Producer(self.producer_conf)
            self.consumer = Consumer(self.consumer_conf)
        else:
            self.producer = None
            self.consumer = None

    def __kafka_monitor_process(self):
        # monitor kafka for incoming messages and respond with results to corresponding jobids
        # This is a SYNCHRONOUS blocking function that runs in a thread

        self.consumer.subscribe(["pl_job_results_0"])

        while True:
            # go though the queue of job ids and check if we have results for them
            for job_id in list(
                self.kakfa_job_ids
            ):  # Use list() to avoid modification during iteration
                if job_id in self.results:
                    # we have results for this job
                    result_message = {
                        "job_id": job_id,
                        "status": "completed",
                    }
                    self.producer.produce(
                        "pl_job_results", key=job_id, value=json.dumps(result_message)
                    )
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
                message_value = msg.value().decode("utf-8")
                message = json.loads(message_value)
                print(f"Received Kafka message: {message}")

                job_id = message.get("job_id")

                if job_id in self.results:
                    # we have results for this job
                    result_message = {
                        "job_id": job_id,
                        "status": "completed",
                    }
                    self.producer.produce(
                        "wrangler_results", key=job_id, value=json.dumps(result_message)
                    )
                    print(f"Emitted results for job {job_id} to kafka.")
                    self.producer.flush()
                else:
                    self.kakfa_job_ids.append(job_id)
                    print(f"No results found for job {job_id}, added to queue")
            except Exception as e:
                print(f"Error processing Kafka message: {e}")

            time.sleep(
                0.1
            )  # Use time.sleep instead of asyncio.sleep since this is sync

    def start_kafka_monitor(self):
        if not self.kafka_enabled:
            print("Running without Kafka - Kafka monitor not started")
            return

        # Start the kafka monitor in a separate thread
        import threading

        self.kafka_thread = threading.Thread(
            target=self.__kafka_monitor_process, daemon=True
        )
        self.kafka_thread.start()
        print("Kafka monitor thread started")

    def stop_kafka_monitor(self):
        if not self.kafka_enabled:
            return
        self.consumer.close()
        self.producer.flush()
        
    def add_kafka_job_id(self, job_id: str):
        # Add a job id to the kafka job ids list if not already present
        if job_id not in self.kakfa_job_ids:
            self.kakfa_job_ids.append(job_id)


# click command group
@click.group()
def cli():
    """CLI for the wrangler service."""
    pass


@cli.command("run")
@click.option("--host", default="0.0.0.0", help="Host for the FastAPI server")
@click.option("--port", default=8000, help="Port for the FastAPI server")
@click.option(
    "--kafka-broker", default="athena.isis.rl.ac.uk:9092", help="Kafka brokers"
)
@click.option(
    "--no-kafka", is_flag=True, default=False, help="Disable Kafka functionality"
)
def run_server(
    host: str = "0.0.0.0",
    port: int = 1001,
    kafka_broker: str = "athena.isis.rl.ac.uk:9092",
    no_kafka: bool = False,
):
    """Run the wrangler FastAPI server."""
    app = FastAPI(title="Wrangler Service", version="1.0.0")

    # Add CORS middleware to allow cross-origin requests (e.g., from test UI)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    server = WranglerServer(kafka_broker, kafka_enabled=not no_kafka)
    server.start_kafka_monitor()

    # Setup routes
    setup_v1_routes(app, server)
    setup_v2_routes(app, server)

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    cli()

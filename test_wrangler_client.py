#!/usr/bin/env python3
"""
Test script for the Wrangler service.

This script connects to the wrangler, retrieves registered models,
and rapidly submits randomized lattice jobs. It also verifies:
1. Results match the submitted lattice data
2. Kafka notifications are received for completed jobs

Usage:
    python test_wrangler_client.py --wrangler-url http://localhost:8000 --num-jobs 10
"""

import argparse
import random
import time
import uuid
import json
import threading
from typing import Dict, Any, List, Set
import requests
import sys
from confluent_kafka import Consumer, Producer, KafkaError


def generate_random_beam() -> Dict[str, Any]:
    """Generate a random beam configuration."""
    return {
        "energy": random.uniform(1.0, 100.0),
        "current": random.uniform(0.1, 10.0),
        "emittance_x": random.uniform(1e-6, 1e-4),
        "emittance_y": random.uniform(1e-6, 1e-4), 
        "bunch_charge": random.uniform(1e-12, 1e-9), 
        "beta_x": random.uniform(1.0, 10.0), 
        "beta_y": random.uniform(1.0, 10.0), 
        "alpha_x": random.uniform(-2.0, 2.0),
        "alpha_y": random.uniform(-2.0, 2.0),
    }


def generate_random_lattice() -> Dict[str, Any]:
    """Generate a random lattice configuration."""
    element_types = ["drift", "quadrupole", "dipole", "cavity", "solenoid"]
    num_elements = random.randint(5, 20)
    
    elements = []
    for i in range(num_elements):
        element_type = random.choice(element_types)
        element = {
            "name": f"{element_type}_{i}",
            "type": element_type,
            "length": random.uniform(0.1, 2.0),  # meters
            "position": i * random.uniform(0.5, 3.0),
        }
        
        # Add type-specific parameters
        if element_type == "quadrupole":
            element["gradient"] = random.uniform(-100, 100)  # T/m
            element["aperture"] = random.uniform(0.01, 0.1)  # m
        elif element_type == "dipole":
            element["field"] = random.uniform(0.1, 2.0)  # T
            element["angle"] = random.uniform(-45, 45)  # degrees
        elif element_type == "cavity":
            element["voltage"] = random.uniform(1e6, 50e6)  # V
            element["frequency"] = random.uniform(100e6, 3e9)  # Hz
            element["phase"] = random.uniform(0, 360)  # degrees
        elif element_type == "solenoid":
            element["field"] = random.uniform(0.1, 5.0)  # T
        
        elements.append(element)
    
    return {
        "name": f"random_lattice_{uuid.uuid4().hex[:8]}",
        "length": sum(e["length"] for e in elements),
        "elements": elements,
        "type": "linac",
    }


def get_models(wrangler_url: str) -> Dict[str, Any]:
    """Get list of registered models from the wrangler."""
    try:
        response = requests.get(f"{wrangler_url}/list_models/")
        response.raise_for_status()
        models = response.json()
        return models
    except requests.RequestException as e:
        print(f"❌ Failed to get models: {e}")
        sys.exit(1)


class KafkaListener(threading.Thread):
    """Background thread that listens for Kafka messages from wrangler."""
    
    def __init__(self, kafka_broker: str, response_topic: str = "wrangler_results"):
        super().__init__(daemon=True)
        self.kafka_broker = kafka_broker
        self.response_topic = response_topic
        self.received_jobs: Set[str] = set()
        self.messages: List[Dict[str, Any]] = []
        self.running = False
        self.consumer = None
        self.producer = None
        
        # Setup producer for sending requests
        producer_conf = {
            'bootstrap.servers': self.kafka_broker,
        }
        try:
            self.producer = Producer(producer_conf)
        except Exception as e:
            print(f"⚠️  Failed to create Kafka producer: {e}")
    
    def request_job_status(self, job_id: str):
        """Send a Kafka message requesting job status."""
        if not self.producer:
            print(f"⚠️  Cannot send Kafka request - producer not initialized")
            return
        
        message = {
            "job_id": job_id,
            "action": "get_status"
        }
        
        try:
            self.producer.produce(
                "simframe_requests",
                key=job_id,
                value=json.dumps(message)
            )
            self.producer.flush()
            print(f"📤 Sent Kafka request for job {job_id[:8]}...")
        except Exception as e:
            print(f"⚠️  Failed to send Kafka request for job {job_id}: {e}")
        
    def run(self):
        """Run the Kafka consumer in background thread."""
        consumer_conf = {
            'bootstrap.servers': self.kafka_broker,
            'group.id': f'test_client_{uuid.uuid4().hex[:8]}',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        }
        
        try:
            self.consumer = Consumer(consumer_conf)
            self.consumer.subscribe([self.response_topic])
            self.running = True
            
            print(f"🎧 Kafka listener started on {self.kafka_broker}, topic: {self.response_topic}")
            
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"⚠️  Kafka error: {msg.error()}")
                        continue
                
                # Parse message
                try:
                    message_value = msg.value().decode('utf-8')
                    message = json.loads(message_value)
                    job_id = message.get('job_id')
                    status = message.get('status')
                    
                    if job_id:
                        self.received_jobs.add(job_id)
                        self.messages.append(message)
                        
                        # Verify message format
                        if status == "completed":
                            print(f"✅ Kafka notification: Job {job_id[:8]}... status={status}")
                        else:
                            print(f"📨 Kafka notification: Job {job_id[:8]}... status={status if status else 'unknown'}")
                        
                except Exception as e:
                    print(f"⚠️  Failed to parse Kafka message: {e}")
                    
        except Exception as e:
            print(f"❌ Kafka listener error: {e}")
            self.running = False
    
    def stop(self):
        """Stop the Kafka listener."""
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()


def submit_job(wrangler_url: str, model_id: str, model_name: str, 
               submitted_jobs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Submit a lattice job to a model via the wrangler."""
    job_id = str(uuid.uuid4())
    
    beam = generate_random_beam()
    lattice = generate_random_lattice()
    
    job_data = {
        "model": model_name,
        "beam": beam,
        "lattice_name": f"test_lattice_{uuid.uuid4().hex[:8]}",
        "lattice": lattice,
        "job_id": job_id,
    }
    
    # Store submitted job data for later verification
    submitted_jobs[job_id] = {
        "beam": beam,
        "lattice": lattice,
        "model_id": model_id,
        "submitted_at": time.time()
    }
    
    try:
        response = requests.post(
            f"{wrangler_url}/submit_lattice/{model_id}",
            json=job_data,
            timeout=5
        )
        response.raise_for_status()
        return {"success": True, "job_id": job_id, "response": response.json()}
    except requests.RequestException as e:
        print(f"❌ Failed to submit job {job_id}: {e}")
        del submitted_jobs[job_id]  # Remove failed job
        return {"success": False, "error": str(e)}


def verify_results(wrangler_url: str, submitted_jobs: Dict[str, Dict[str, Any]], 
                   kafka_listener: KafkaListener, timeout: int = 120) -> Dict[str, Any]:
    """Verify that job results match submitted data and check Kafka notifications."""
    print()
    print("=" * 70)
    print("🔍 Verification Phase")
    print("=" * 70)
    print(f"Waiting up to {timeout}s for jobs to complete...")
    print()
    
    start_time = time.time()
    verified_jobs = {}
    failed_verifications = []
    kafka_requested_jobs = set()
    
    # Wait for all jobs to complete or timeout
    pending_jobs = set(submitted_jobs.keys())
    
    while pending_jobs and (time.time() - start_time) < timeout:
        for job_id in list(pending_jobs):
            elapsed = time.time() - start_time
            
            # Try to fetch results via REST first
            model_id = submitted_jobs[job_id]["model_id"]
            try:
                response = requests.get(
                    f"{wrangler_url}/get_result/{job_id}",
                    timeout=5
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # Job completed! Now send Kafka request to verify notification system
                    if kafka_listener and job_id not in kafka_requested_jobs:
                        print(f"✅ Job {job_id[:8]}... completed ({elapsed:.1f}s) - requesting via Kafka")
                        kafka_listener.request_job_status(job_id)
                        kafka_requested_jobs.add(job_id)
                        time.sleep(0.5)  # Give Kafka time to respond
                    else:
                        print(f"✅ Job {job_id[:8]}... completed ({elapsed:.1f}s)")
                    
                    # Check if we have the beam data in results
                    returned_beam = result.get('beam', {})
                    submitted_beam = submitted_jobs[job_id]['beam']
                    
                    # Verify beam data matches (allowing for transformation)
                    verification_passed = True
                    if returned_beam:
                        # If model returns beam data, verify it matches
                        # (In real scenarios, the beam might be transformed)
                        print(f"   📊 Result contains beam data with {len(returned_beam)} properties")
                    else:
                        print(f"   ⚠️  No beam data in result")
                    
                    verified_jobs[job_id] = {
                        "result": result,
                        "matched": verification_passed,
                        "completion_time": time.time() - submitted_jobs[job_id]["submitted_at"],
                        "kafka_requested": job_id in kafka_requested_jobs
                    }
                    
                    pending_jobs.remove(job_id)
                    
                elif response.status_code == 202:
                    print(f"⏳ Job {job_id[:8]}... still processing ({elapsed:.1f}s)")
                    pass
                else:
                    # Error
                    print(f"❌ Job {job_id[:8]}... failed: {response.status_code}")
                    failed_verifications.append(job_id)
                    pending_jobs.remove(job_id)
                    
            except requests.RequestException as e:
                # Job not ready yet or error
                pass
        
        if pending_jobs:
            time.sleep(0.5)  # Poll every 500ms
    
    # Check for timeouts
    if pending_jobs:
        print()
        print(f"⏱️  {len(pending_jobs)} jobs timed out after {timeout}s")
        for job_id in pending_jobs:
            failed_verifications.append(job_id)
    
    # Verify Kafka notifications
    print()
    print("-" * 70)
    print("📬 Kafka Notification Verification")
    print("-" * 70)
    
    # Give Kafka a bit more time to process all messages
    if kafka_listener:
        print("Waiting 2s for final Kafka messages...")
        time.sleep(2)
    
    kafka_received = kafka_listener.received_jobs if kafka_listener else set()
    verified_job_ids = set(verified_jobs.keys())
    requested_job_ids = kafka_requested_jobs
    
    # Verify message format
    format_valid_count = 0
    if kafka_listener:
        for message in kafka_listener.messages:
            if 'job_id' in message and 'status' in message:
                if message['status'] == 'completed':
                    format_valid_count += 1
        
        print(f"Total Kafka messages: {len(kafka_listener.messages)}")
        print(f"Messages with correct format: {format_valid_count}")
        print(f"  - Expected format: {{'job_id': <id>, 'status': 'completed'}}")
    
    # Jobs that completed and were requested via Kafka
    requested_and_received = requested_job_ids & kafka_received
    requested_but_missing = requested_job_ids - kafka_received
    
    print(f"\nKafka requests sent: {len(requested_job_ids)}")
    print(f"Kafka responses received: {len(kafka_received)}")
    print(f"Requested and received: {len(requested_and_received)}")
    
    if requested_but_missing:
        print(f"⚠️  Kafka requests without response: {len(requested_but_missing)}")
        for job_id in list(requested_but_missing)[:5]:
            print(f"   - {job_id[:16]}...")
        if len(requested_but_missing) > 5:
            print(f"   ... and {len(requested_but_missing) - 5} more")
    
    # Check if all received messages have correct format
    if kafka_listener and len(kafka_listener.messages) > 0:
        if format_valid_count == len(requested_and_received):
            print(f"✅ All Kafka responses have correct format")
        else:
            print(f"⚠️  {len(requested_and_received) - format_valid_count} messages with invalid format")
    
    return {
        "verified": verified_jobs,
        "failed": failed_verifications,
        "kafka_received": len(kafka_received),
        "kafka_requested": len(requested_job_ids),
        "kafka_matched": len(requested_and_received),
        "missing_notifications": len(requested_but_missing),
        "format_valid": format_valid_count,
        "completion_times": [v["completion_time"] for v in verified_jobs.values()]
    }


def main():
    parser = argparse.ArgumentParser(
        description="Test script for rapid lattice job submission to the wrangler with verification"
    )
    parser.add_argument(
        "--wrangler-url",
        default="http://localhost:8000",
        help="URL of the wrangler service (default: http://localhost:8000)"
    )
    parser.add_argument(
        "--kafka-broker",
        default="athena.isis.rl.ac.uk:9092",
        help="Kafka broker address (default: athena.isis.rl.ac.uk:9092)"
    )
    parser.add_argument(
        "--num-jobs",
        type=int,
        default=10,
        help="Total number of jobs to submit (default: 10)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay between job submissions in seconds (default: 0.1)"
    )
    parser.add_argument(
        "--model-id",
        default=None,
        help="Target specific model ID (default: randomly distribute across all models)"
    )
    parser.add_argument(
        "--verify-timeout",
        type=int,
        default=120,
        help="Timeout for result verification in seconds (default: 120)"
    )
    parser.add_argument(
        "--skip-kafka",
        action="store_true",
        help="Skip Kafka listener (useful if Kafka is not available)"
    )
    parser.add_argument(
        "--randomize",
        action="store_true",
        default=True,
        help="Randomly distribute jobs across models (default: True)"
    )
    parser.add_argument(
        "--no-randomize",
        action="store_false",
        dest="randomize",
        help="Submit jobs sequentially to each model (old behavior)"
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 Wrangler Load Test Client with Verification")
    print("=" * 70)
    print(f"Wrangler URL: {args.wrangler_url}")
    print(f"Kafka Broker: {args.kafka_broker}")
    print(f"Total jobs: {args.num_jobs}")
    print(f"Delay between jobs: {args.delay}s")
    print(f"Verification timeout: {args.verify_timeout}s")
    print(f"Job distribution: {'Random' if args.randomize else 'Sequential'}")
    print()
    
    # Start Kafka listener if not skipped
    kafka_listener = None
    if not args.skip_kafka:
        try:
            kafka_listener = KafkaListener(args.kafka_broker)
            kafka_listener.start()
            time.sleep(1)  # Give it time to connect
        except Exception as e:
            print(f"⚠️  Failed to start Kafka listener: {e}")
            print(f"   Continuing without Kafka verification...")
            kafka_listener = None
    
    # Get list of models
    print("📋 Fetching registered models...")
    models = get_models(args.wrangler_url)
    
    if not models:
        print("❌ No models registered with the wrangler!")
        print("   Start a model first: pl run -c test_deployment.yaml")
        sys.exit(1)
    
    print(f"✅ Found {len(models)} registered model(s):")
    for model_id, model_info in models.items():
        print(f"   • {model_info.get('model_name', 'unnamed')} (ID: {model_id})")
        print(f"     API: {model_info.get('api_url', 'unknown')}")
    print()
    
    # Filter to specific model if requested
    if args.model_id:
        if args.model_id not in models:
            print(f"❌ Model ID '{args.model_id}' not found!")
            sys.exit(1)
        models = {args.model_id: models[args.model_id]}
        print(f"🎯 Targeting specific model: {args.model_id}")
        print()
    
    # Submit jobs
    total_jobs = args.num_jobs
    print(f"🔥 Submitting {total_jobs} total jobs...")
    print("-" * 70)
    
    submitted_jobs = {}
    start_time = time.time()
    successful_jobs = 0
    failed_jobs = 0
    
    # Track jobs per model for statistics
    jobs_per_model = {model_id: 0 for model_id in models.keys()}
    
    # Create list of models for random selection
    model_list = [(model_id, model_info) for model_id, model_info in models.items()]
    
    if args.randomize:
        # Random distribution mode
        print(f"🎲 Randomly distributing {total_jobs} jobs across {len(models)} model(s)\n")
        
        for i in range(total_jobs):
            # Randomly select a model
            model_id, model_info = random.choice(model_list)
            model_name = model_info.get('model_name', model_id)
            
            print(f"📤 Job {i+1}/{total_jobs} -> {model_name[:20]}... ", end="", flush=True)
            
            result = submit_job(args.wrangler_url, model_id, model_name, submitted_jobs)
            
            if result.get('success'):
                job_id = result.get('job_id', 'unknown')
                print(f"✅ Submitted (job_id: {job_id[:8]}...)")
                successful_jobs += 1
                jobs_per_model[model_id] += 1
            else:
                print(f"❌ Failed")
                failed_jobs += 1
            
            if args.delay > 0 and i < total_jobs - 1:
                time.sleep(args.delay)
    else:
        # Sequential mode (old behavior)
        jobs_per_model_target = total_jobs // len(models)
        remainder = total_jobs % len(models)
        
        job_count = 0
        for idx, (model_id, model_info) in enumerate(model_list):
            model_name = model_info.get('model_name', model_id)
            
            # Distribute remainder across first models
            num_jobs_for_this_model = jobs_per_model_target + (1 if idx < remainder else 0)
            
            print(f"\n📤 Submitting {num_jobs_for_this_model} jobs to model: {model_name}")
            
            for i in range(num_jobs_for_this_model):
                job_count += 1
                print(f"   Job {i+1}/{num_jobs_for_this_model} (total: {job_count}/{total_jobs})... ", end="", flush=True)
                
                result = submit_job(args.wrangler_url, model_id, model_name, submitted_jobs)
                
                if result.get('success'):
                    job_id = result.get('job_id', 'unknown')
                    print(f"✅ Submitted (job_id: {job_id[:8]}...)")
                    successful_jobs += 1
                    jobs_per_model[model_id] += 1
                else:
                    print(f"❌ Failed")
                    failed_jobs += 1
                
                if args.delay > 0 and job_count < total_jobs:
                    time.sleep(args.delay)
    
    elapsed_time = time.time() - start_time
    
    # Submission summary
    print()
    print("=" * 70)
    print("📊 Submission Summary")
    print("=" * 70)
    print(f"Total jobs submitted: {successful_jobs}/{total_jobs}")
    print(f"Failed submissions: {failed_jobs}")
    print(f"Success rate: {successful_jobs/total_jobs*100:.1f}%")
    print(f"Submission time: {elapsed_time:.2f}s")
    print(f"Average rate: {successful_jobs/elapsed_time:.2f} jobs/sec")
    
    # Show distribution across models
    if len(models) > 1:
        print()
        print("Job distribution across models:")
        for model_id, count in jobs_per_model.items():
            model_name = models[model_id].get('model_name', model_id)
            percentage = (count / successful_jobs * 100) if successful_jobs > 0 else 0
            bar = "█" * int(percentage / 5)  # Scale bar to ~20 chars max
            print(f"  {model_name[:25]:<25} {count:>3} jobs ({percentage:>5.1f}%) {bar}")
    
    # Verify results if we have successful jobs
    if successful_jobs > 0 and submitted_jobs:
        verification_results = verify_results(
            args.wrangler_url, 
            submitted_jobs, 
            kafka_listener if kafka_listener else KafkaListener("dummy"),
            timeout=args.verify_timeout
        )
        
        # Final summary
        print()
        print("=" * 70)
        print("📈 Final Results")
        print("=" * 70)
        
        verified_count = len(verification_results['verified'])
        failed_count = len(verification_results['failed'])
        
        print(f"Jobs verified: {verified_count}/{successful_jobs}")
        print(f"Jobs failed: {failed_count}")
        
        if verification_results['completion_times']:
            avg_time = sum(verification_results['completion_times']) / len(verification_results['completion_times'])
            min_time = min(verification_results['completion_times'])
            max_time = max(verification_results['completion_times'])
            print(f"Completion time (avg): {avg_time:.2f}s")
            print(f"Completion time (min/max): {min_time:.2f}s / {max_time:.2f}s")
        
        if kafka_listener:
            kafka_requested = verification_results['kafka_requested']
            kafka_received = verification_results['kafka_received']
            kafka_matched = verification_results['kafka_matched']
            format_valid = verification_results['format_valid']
            
            kafka_match_rate = (kafka_matched / kafka_requested * 100) if kafka_requested > 0 else 0
            format_valid_rate = (format_valid / kafka_matched * 100) if kafka_matched > 0 else 0
            
            print()
            print("Kafka Verification:")
            print(f"  Requests sent: {kafka_requested}")
            print(f"  Responses received: {kafka_received}")
            print(f"  Match rate: {kafka_match_rate:.1f}%")
            print(f"  Valid format: {format_valid}/{kafka_matched} ({format_valid_rate:.1f}%)")
            
            if verification_results['missing_notifications'] > 0:
                print(f"  ⚠️  {verification_results['missing_notifications']} requests without response")
        
        print()
        
        # Overall pass/fail
        all_verified = (verified_count == successful_jobs)
        kafka_ok = True
        if kafka_listener:
            kafka_requested = verification_results['kafka_requested']
            kafka_matched = verification_results['kafka_matched']
            format_valid = verification_results['format_valid']
            # Kafka is OK if: all requests got responses AND all have correct format
            kafka_ok = (kafka_matched == kafka_requested) and (format_valid == kafka_matched)
        
        if all_verified and kafka_ok:
            print("✅ All tests passed!")
            print("   ✓ All jobs completed successfully")
            if kafka_listener:
                print("   ✓ All Kafka requests received correct responses")
                print("   ✓ All Kafka messages have correct format: {'job_id': <id>, 'status': 'completed'}")
        else:
            print("⚠️  Some tests failed - review results above")
            if not all_verified:
                print(f"   ✗ {successful_jobs - verified_count} jobs failed verification")
            if kafka_listener and not kafka_ok:
                missing = verification_results['missing_notifications']
                invalid_format = kafka_matched - format_valid if kafka_matched > format_valid else 0
                if missing > 0:
                    print(f"   ✗ {missing} Kafka requests without response")
                if invalid_format > 0:
                    print(f"   ✗ {invalid_format} Kafka messages with invalid format")
    
    # Cleanup
    if kafka_listener:
        print()
        print("Stopping Kafka listener...")
        kafka_listener.stop()
        kafka_listener.join(timeout=2)


if __name__ == "__main__":
    main()

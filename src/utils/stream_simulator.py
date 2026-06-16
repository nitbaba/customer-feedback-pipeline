import os
import time
import json
import random
from generator import generate_mock_ticket

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) #src/utils
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../")) #customer-feedback-pipeline

LANDING_DIR = os.path.join(PROJECT_ROOT, "data_lake", "landing")

def run_simulator(batch_count=10, delay_seconds=3):
    """
    Simulate stream of data by writing batches of JSON files to local landing dir
    """
    os.makedirs(LANDING_DIR, exist_ok=True)

    print(f"starting stream sim. Writing to '{LANDING_DIR}'...")
    print(f"send sigkill(ctrl+c) to stop.\n")

    batch_id = 1

    try:
        while True:
            print(f"Generating Batch #{batch_id}")

            #generate random number of tickets
            tickets_in_batch = random.randint(3, 7)
            batch_data = []

            for i in range(tickets_in_batch):
                batch_data.append(generate_mock_ticket())

            #create unique filename for batch
            timestamp = int(time.time())
            filename = f"batch_{timestamp}_{batch_id}.json"
            filepath = os.path.join(LANDING_DIR, filename)

            #write the data as JSON
            with open(filepath, "w") as f:
                json.dump(batch_data, f, indent=2)

            print(f"Success, wrote {tickets_in_batch} tickets to {filename}")

            batch_id += 1
            #wait for time before next batch
            time.sleep(delay_seconds)

    except KeyboardInterrupt:
        print("\n Simulator stopped by user.")

if __name__ == "__main__":
    run_simulator(delay_seconds=3)
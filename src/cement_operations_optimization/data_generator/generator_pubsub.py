import json, random, time
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT = "cement-operations-optimization"
TOPIC = "cement-raw"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT, TOPIC)

def generate_record():
    equipment = random.choice(["Kiln","Cement Mill","Preheater","Cooler","Raw Mill"])
    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "equipment": equipment,
        "temperature": round(random.uniform(1200,1450),2),
        "pressure": round(random.uniform(50,100),2),
        "vibration": round(random.uniform(0.1,5.0),2),
        "power": round(random.uniform(400,600),2),
        "emission": round(random.uniform(50,150),2),
        "anomaly": False,
        "anomaly_type": None
    }
    if random.random() < 0.1:
        record["anomaly"] = True
        record["anomaly_type"] = "Kiln temperature spike"
        record["temperature"] = round(random.uniform(1500,1650),2)
    return record

def main():
    while True:
        msg = json.dumps(generate_record()).encode("utf-8")
        publisher.publish(topic_path, msg)
        time.sleep(1)

if __name__ == "__main__":
    main()

from confluent_kafka import Producer, Consumer

def create_producer(bootstrap):
    return Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 5
    })

def create_consumer(bootstrap, group_id, topics):
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "latest"
    })
    c.subscribe(topics)
    return c

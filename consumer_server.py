import asyncio

from confluent_kafka import Consumer 
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL="PLAINTEXT://localhost:9092"

async def consumer(topic_name):
    # Build consumer
    c = Consumer({"bootstrap.servers": BROKER_URL,
                  "group.id": "consgroup"})
    c.subscribe([topic_name])
    # Basic poll loop
    while True:
        msg = c.poll(0.1)
        if msg is None: pass
        elif msg.error(): print('ERROR', msg.error())
        else: print(msg.value())
        await asyncio.sleep(0.05)

def main():
    try:
        asyncio.run(consumer("police_calls"))
    except KeyboardInterrupt as e:
        pass
        
if __name__ == '__main__':
    main()
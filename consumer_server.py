from kafka import KafkaConsumer

BROKER_URL = "localhost:9092"

def consume(topic_name):
    c = KafkaConsumer(bootstrap_servers=BROKER_URL,
                      group_id="org.sanfranciscopolice.pyconsumers",
                      auto_offset_reset="earliest"
        )
    c.subscribe([topic_name])
    
    for pol_msg in c:
        print(pol_msg.value)

        
if __name__ == "__main__":
    consume("org.sanfranciscopolice.crime")
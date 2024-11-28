import sys
import getopt
import random
from time import sleep
from kafka import KafkaProducer

def usage():
    print(f"Usage: {sys.argv[0]} <bootstrap_servers> <topic> [-i <interval>] [-k <keyprefix> ...]")

def main(argv):
    if len(argv) < 2:
        usage()
        sys.exit(2)

    bootstrap_servers = argv[0]
    topic = argv[1]
    interval = 1
    krange = 100
    vrange = 2000
    keys = []

    try:
        opts, args = getopt.getopt(argv[2:], "i:k:r:v:", ["interval=", "key=", "range=", "value="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-i", "--interval"):
            interval = float(arg)
        elif opt in ("-k", "--key"):
            keys.append(arg)
        elif opt in ("-r", "--range"):
            krange = int(arg)
        elif opt in ("-v", "--value"):
            vrange = int(arg)
    if not keys:
        keys.append("key")

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers.split(','))

    try:
        while True:
            key = random.choice(keys) + str(random.randint(1, krange))
            value = random.choice(["good-", "bad-"]) + str(random.randint(1, vrange))
            producer.send(topic, key=key.encode(), value=value.encode())
            print(f"Sent: key={key}\tvalue={value}")
            sleep(interval)
    except KeyboardInterrupt:
        print("Shutting down.")
    finally:
        producer.close()

if __name__ == "__main__":
    main(sys.argv[1:])
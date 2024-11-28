import sys
import getopt
import struct
from kafka import KafkaConsumer

def usage():
    print(f"Usage: {sys.argv[0]} <bootstrap_servers> <topic> [-g <group>]")

def des_long(data):
    if data is None:
        return None
    if len(data) != 8:
        return data
    return struct.unpack('>q', data)[0]

def main(argv):
    if len(argv) < 2:
        usage()
        sys.exit(2)

    bootstrap_servers = argv[0]
    topic = argv[1]
    group = None
    debug = False
    desstring = False

    try:
        opts, args = getopt.getopt(argv[2:], "g:ds", ["group=","debug","string"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-g", "--group"):
            group = arg
        if opt in ("-d", "--debug"):
            debug = True
        if opt in ("-s", "--string"):
            desstring = True

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(','),
        group_id=group,
        client_id="consumer",
        auto_offset_reset='earliest',

        key_deserializer=lambda m: m.decode('utf-8') if m else None,
        value_deserializer=des_long if not desstring else lambda m: m.decode('utf-8'),
    )

    try:
        for message in consumer:
            if debug:
                print(repr(message))
            else:
                try:
                    print(f"[{message.topic}/{message.partition}]@{message.offset}\tKey: {message.key}\t{message.value}")
                except:
                    print(repr(message))
    except KeyboardInterrupt:
        print("Shutting down.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main(sys.argv[1:])

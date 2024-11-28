import sys
import getopt
import random
import struct
from time import sleep
from kafka import KafkaProducer

words = [
    "apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon",
    "mango", "nectarine", "orange", "papaya", "quince", "raspberry", "strawberry", "tangerine", "ugli", "vanilla",
    "watermelon", "xigua", "yam", "zucchini", "apricot", "blackberry", "blueberry", "cantaloupe", "cranberry", "dragonfruit",
    "gooseberry", "grapefruit", "guava", "huckleberry", "jackfruit", "kumquat", "lime", "lychee", "mulberry", "olive",
    "passionfruit", "peach", "pear", "persimmon", "pineapple", "plum", "pomegranate", "pomelo", "rambutan", "redcurrant",
    "starfruit", "tamarind", "tomato", "boysenberry", "cloudberry", "durian", "feijoa", "jabuticaba", "jambul", "jostaberry",
    "kiwifruit", "langsat", "longan", "loquat", "madrono", "mangosteen", "marionberry", "medlar", "miracle", "nance",
    "nance", "naranjilla", "pecan", "pequi", "pitanga", "pitaya", "plantain", "pulasan", "purple", "rambai",
    "salak", "santol", "satsuma", "soursop", "surinam", "tangelo", "tayberry", "velvet", "voavanga", "white",
    "yunnan", "zinfandel", "ziziphus", "acerola", "akee", "ackee", "ambarella", "bignay", "bilberry", "biriba"
]

def usage():
    print(f"Usage: {sys.argv[0]} <bootstrap_servers> <topic>")

def main(argv):
    if len(argv) < 2:
        usage()
        sys.exit(2)

    bootstrap_servers = argv[0]
    topic = argv[1]

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers.split(','))

    try:
        for i, w in enumerate(words):
            producer.send(topic, key=struct.pack('>q', i), value=w.encode())
            print(f"Sent: key={i}\tvalue={w}")

    except KeyboardInterrupt:
        print("Shutting down.")
    finally:
        producer.close()

if __name__ == "__main__":
    main(sys.argv[1:])
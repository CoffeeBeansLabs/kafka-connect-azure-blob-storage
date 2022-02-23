from json import dumps
from random import randrange
from kafka import KafkaProducer
from random_word import RandomWords

producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
                         value_serializer=lambda x: dumps(x).encode('UTF-8'))

r = RandomWords()
blobNames = ['blob-1', 'blob-2', 'blob-3', 'blob-4', 'blob-5']
randomWords = r.get_random_words(limit=500)

for e in range(499):
    data = {
        'name': blobNames[randrange(0, 3, 1)],
        'num': e,
        'word': randomWords[e]
    }
    producer.send('connect-demo', data)
    print(data)

import random
import requests
import json

mapping = json.loads(open('../../website/queries/mapping.json').read())

for id, pair in enumerate(mapping):
    response = requests.post('http://localhost:8080/bigdawg/query',
                             headers={'Content-type': 'application/json', 'Accept': 'application/json'},
                             data=json.dumps({'query': 'ARRAY(subarray(regrid(filter(slice(waveform_signal_table, RecordName, {}1), signal != 0 and signal != nan), 256, avg(signal) as signal), 0, 255))'.format(pair['waveform'])}))
    for time, signal in response.json()['tuples']:
        print "%d,%d,%f" % (id, int(time), max(-1.0, min(1.0, float(signal))))

for id in xrange(len(mapping), 600):
    for time in xrange(0, 256):
        print "%d,%d,%f" % (id, int(time), random.uniform(-1, 1))



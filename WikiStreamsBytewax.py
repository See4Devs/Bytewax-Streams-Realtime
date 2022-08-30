import json
from datetime import timedelta
import sseclient
import urllib3
from bytewax import Dataflow, inputs, parse, spawn_cluster
from bytewax.inputs import ManualInputConfig

def wikiStreamInput():
    wikiPool = urllib3.PoolManager()
    responseData = wikiPool.request(
        "GET",
        "https://stream.wikimedia.org/v2/stream/recentchange/",
        preload_content=False,
        headers={"Accept": "text/event-stream"},
    )
    WikiEvents = sseclient.SSEClient(responseData)
    for event in WikiEvents.events():
        yield event.data

def outputBuilder(worker_index, worker_count):
    return print

def filter_france(data):
    if data['server_name']=='fr.wikipedia.org':
        with open('WikiEdits.txt', 'a') as f:
            f.write(str(data)+"\n")
        return data, 1

@inputs.yield_epochs
def inputBuilder(workerIndex, workerCount, resumeEpoch):
    if workerIndex == 0:
        return inputs.tumbling_epoch(
            wikiStreamInput(),
            timedelta(seconds=2),
            epoch_start=resumeEpoch,
        )
    else:
        return []

flow = Dataflow()
flow.map(json.loads)
flow.map(filter_france)
flow.capture()

if __name__ == "__main__":
    spawn_cluster(
        flow, ManualInputConfig(inputBuilder), outputBuilder, **parse.cluster_args()
    )
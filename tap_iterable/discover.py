
# 
# Module dependencies.
# 

import os
import json
import singer
from tap_iterable.streams import STREAMS


def discover_streams(client):
    streams = []

    for s in STREAMS.values():
        s = s(client)
        schema = singer.resolve_schema_references(s.load_schema())
        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': schema, 'metadata': s.load_metadata()})
    return streams





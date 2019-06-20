
# 
# Module dependencies.
# 

import os
import json
import datetime
import pytz
import singer
import time
from singer import metadata
from singer import utils
from singer.metrics import Point
from dateutil.parser import parse
from tap_iterable.context import Context


logger = singer.get_logger()
KEY_PROPERTIES = ['id']


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def epoch_to_datetime_string(milliseconds):
    datetime_string = None
    try:
        datetime_string = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(milliseconds / 1000))
    except TypeError:
        # If fails, it means format already datetime string.
        datetime_string = milliseconds
        pass
    return datetime_string


class Stream():
    name = None
    replication_method = None
    replication_key = None
    stream = None
    key_properties = KEY_PROPERTIES
    session_bookmark = None


    def __init__(self, client=None):
        self.client = client


    def is_session_bookmark_old(self, value):
        if self.session_bookmark is None:
            return True
        # Assume value is in epoch milliseconds.
        value_in_date_time = epoch_to_datetime_string(value)
        return utils.strptime_with_tz(value_in_date_time) > utils.strptime_with_tz(self.session_bookmark)


    def update_session_bookmark(self, value):
        # Assume value is epoch milliseconds.
        value_in_date_time = epoch_to_datetime_string(value)
        if self.is_session_bookmark_old(value_in_date_time):
            self.session_bookmark = value_in_date_time


    # Reads and converts bookmark from state.
    def get_bookmark(self, state, name=None):
        name = self.name if not name else name
        return (singer.get_bookmark(state, name, self.replication_key)) or Context.config["start_date"]


    # Converts and writes bookmark to state.
    def update_bookmark(self, state, value, name=None):
        name = self.name if not name else name
        # when `value` is None, it means to set the bookmark to None
        # Assume value is epoch time
        value_in_date_time = epoch_to_datetime_string(value)
        if value_in_date_time is None or self.is_bookmark_old(state, value_in_date_time, name):
            singer.write_bookmark(state, name, self.replication_key, value_in_date_time)


    def is_bookmark_old(self, state, value, name=None):
        # Assume value is epoch time.
        value_in_date_time = epoch_to_datetime_string(value)
        current_bookmark = self.get_bookmark(state, name)
        return utils.strptime_with_tz(value_in_date_time) > utils.strptime_with_tz(current_bookmark)


    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return schema


    def load_metadata(self):
        return metadata.get_standard_metadata(schema=self.load_schema(), 
                                              schema_name=self.name, 
                                              key_properties=self.key_properties, 
                                              valid_replication_keys=[self.replication_key], 
                                              replication_method=self.replication_method)


    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)
        res = get_data(self.replication_key, bookmark)

        if self.replication_method == "INCREMENTAL":
            # These streams results are not ordered, so store highest value bookmark in session.
            for item in res:
                self.update_session_bookmark(item[self.replication_key])
                yield (self.stream, item)
            self.update_bookmark(state, self.session_bookmark)

        else:
            for item in res:
                yield (self.stream, item)


class Lists(Stream):
    name = "lists"
    replication_method = "FULL_TABLE"


class ListUsers(Stream):
    name = "list_users"
    replication_method = "FULL_TABLE"


class Campaigns(Stream):
    name = "campaigns"
    replication_method = "INCREMENTAL"
    replication_key = "updatedAt"


class Channels(Stream):
    name = "channels"
    replication_method = "FULL_TABLE"


class MessageTypes(Stream):
    name = "message_types"
    replication_method = "FULL_TABLE"


class Templates(Stream):
    name = "templates"
    replication_method = "INCREMENTAL"
    replication_key = "updatedAt"
    key_properties = [ "templateId" ]


class Metadata(Stream):
    name = "metadata"
    replication_method = "FULL_TABLE"
    key_properties = [ "key" ]


class EmailBounce(Stream):
    name = "email_bounce"
    replication_method = "INCREMENTAL"
    replication_key = "createdAt"
    key_properties = [ "messageId" ]


class EmailClick(Stream):
    name = "email_click"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]
    replication_key = "createdAt"


class EmailComplaint(Stream):
    name = "email_complaint"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]
    replication_key = "createdAt"


class EmailOpen(Stream):
    name = "email_open"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]
    replication_key = "createdAt"


class EmailSend(Stream):
    name = "email_send"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]
    replication_key = "createdAt"


class EmailSendSkip(Stream):
    name = "email_send_skip"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]
    replication_key = "createdAt"


class EmailSubscribe(Stream):
    name = "email_subscribe"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]
    replication_key = "createdAt"


class EmailUnsubscribe(Stream):
    name = "email_unsubscribe"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]
    replication_key = "createdAt"


class Users(Stream):
    name = "users"
    replication_method = "INCREMENTAL"
    key_properties = [ "userId" ]
    replication_key = "createdAt"


STREAMS = {
    "lists": Lists,
    "list_users": ListUsers,
    "campaigns": Campaigns,
    "channels": Channels,
    "message_types": MessageTypes,
    "templates": Templates,
    "metadata": Metadata,
    "email_bounce": EmailBounce,
    "email_click": EmailClick,
    "email_complaint": EmailComplaint,
    "email_open": EmailOpen,
    "email_send": EmailSend,
    "email_send_skip": EmailSendSkip,
    "email_subscribe": EmailSubscribe,
    "email_unsubscribe": EmailUnsubscribe,
    "users": Users
}



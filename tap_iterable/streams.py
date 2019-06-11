
# 
# Module dependencies.
# 

import os
import json
import datetime
import pytz
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point
from dateutil.parser import parse
from tap_iterable.context import Context


logger = singer.get_logger()
KEY_PROPERTIES = ['id']


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def needs_parse_to_date(string):
    if isinstance(string, str):
        try: 
            parse(string)
            return True
        except ValueError:
            return False
    return False


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
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(self.session_bookmark)


    def update_session_bookmark(self, value):
        if self.is_session_bookmark_old(value):
            self.session_bookmark = value


    def get_bookmark(self, state, name=None):
        name = self.name if not name else name
        return (singer.get_bookmark(state, name, self.replication_key)) or Context.config["start_date"]


    def update_bookmark(self, state, value, name=None):
        name = self.name if not name else name
        # when `value` is None, it means to set the bookmark to None
        if value is None or self.is_bookmark_old(state, value, name):
            singer.write_bookmark(state, name, self.replication_key, value)


    def is_bookmark_old(self, state, value, name=None):
        current_bookmark = self.get_bookmark(state, name)
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(current_bookmark)


    def bookmark_earlier_than(self, item):
        current_bookmark = self.get_bookmark(state, name)
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(current_bookmark)


    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return schema


    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)


    def get_replication_value(self, item):
        return item[self.replication_key]


    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)
        res = get_data(self.replication_key, bookmark)

        for item in res:
            if self.replication_method == "INCREMENTAL":
                if self.is_bookmark_old(state, self.get_replication_value(item)):
                    self.update_session_bookmark(self.get_replication_value(item))
                    yield (self.stream, item)

            else:
                yield (self.stream, item)        

        # After the sync, then set the bookmark based off session_bookmark.
        self.update_bookmark(state, self.session_bookmark)


class Lists(Stream):
    name = "lists"
    replication_method = "FULL_TABLE"


class List_Users(Stream):
    name = "list_users"
    replication_method = "FULL_TABLE"


class Campaigns(Stream):
    name = "campaigns"
    replication_method = "INCREMENTAL"
    replication_key = "updatedAt"


class Channels(Stream):
    name = "channels"
    replication_method = "FULL_TABLE"


class Message_Types(Stream):
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


class Email_Bounce(Stream):
    name = "email_bounce"
    replication_method = "INCREMENTAL"
    replication_key = "createdAt"
    key_properties = [ "messageId" ]


class Email_Click(Stream):
    name = "email_click"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]

    def get_replication_value(self, item):
        return item["itblInternal"]["documentUpdatedAt"]



class Email_Complaint(Stream):
    name = "email_complaint"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]

    def get_replication_value(self, item):
        return item["itblInternal"]["documentUpdatedAt"]



class Email_Open(Stream):
    name = "email_open"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]

    def get_replication_value(self, item):
        return item["itblInternal"]["documentUpdatedAt"]


class Email_Send(Stream):
    name = "email_send"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]

    def get_replication_value(self, item):
        return item["itblInternal"]["documentUpdatedAt"]


class Email_Send_Skip(Stream):
    name = "email_send_skip"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]

    def get_replication_value(self, item):
        return item["itblInternal"]["documentUpdatedAt"]


class Email_Subscribe(Stream):
    name = "email_subscribe"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]

    def get_replication_value(self, item):
        return item["itblInternal"]["documentUpdatedAt"]


class Email_Unsubscribe(Stream):
    name = "email_unsubscribe"
    replication_method = "INCREMENTAL"
    key_properties = [ "messageId" ]

    def get_replication_value(self, item):
        return item["itblInternal"]["documentUpdatedAt"]


class Users(Stream):
    name = "users"
    replication_method = "INCREMENTAL"
    key_properties = [ "userId" ]

    def get_replication_value(self, item):
        return item["itblInternal"]["documentUpdatedAt"]


STREAMS = {
    "lists": Lists,
    "list_users": List_Users,
    "campaigns": Campaigns,
    "channels": Channels,
    "message_types": Message_Types,
    "templates": Templates,
    "metadata": Metadata,
    "email_bounce": Email_Bounce,
    "email_click": Email_Click,
    "email_complaint": Email_Complaint,
    "email_open": Email_Open,
    "email_send": Email_Send,
    "email_send_skip": Email_Send_Skip,
    "email_subscribe": Email_Subscribe,
    "email_unsubscribe": Email_Unsubscribe,
    "users": Users
}



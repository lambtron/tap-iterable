
#
# Module dependencies.
#

from datetime import datetime, timedelta
from singer import utils
from urllib.parse import urlencode
import backoff
import json
import requests
import logging
import time


logger = logging.getLogger()


""" Simple wrapper for Iterable. """
class Iterable(object):

  def __init__(self, api_key, start_date=None):
    self.api_key = api_key
    self.uri = "https://api.iterable.com/api/"


  def _epoch_to_datetime_string(self, milliseconds):
    datetime_string = None
    try:
      datetime_string = time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(milliseconds / 1000))
    except TypeError:
      pass
    return datetime_string


  def _datetime_string_to_epoch(self, datetime_string):
    return utils.strptime_with_tz(datetime_string).timestamp() * 1000


  def _now(self):
    # return datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


  def retry_handler(details):
    logger.info("Received 429 -- sleeping for %s seconds",
                details['wait'])

  # 
  # The actual `get` request.
  # 
  
  @backoff.on_exception(backoff.expo,
                        requests.exceptions.HTTPError,
                        on_backoff=retry_handler,
                        max_tries=10)
  def _get(self, path, stream=False, **kwargs):
    uri = "{uri}{path}".format(uri=self.uri, path=path)
    
    # Add query params, including `api_key`.
    params = { "api_key": self.api_key }
    for key, value in kwargs.items():
      params[key] = value
    uri += "?{params}".format(params=urlencode(params))

    logger.info("GET request to {uri}".format(uri=uri))
    response = requests.get(uri, stream=stream)
    response.raise_for_status()
    return response

  #
  # The common `get` request.
  #
  
  def get(self, path, **kwargs):
    response = self._get(path, **kwargs)
    return response.json()

  #
  # The streaming `get` request.
  # 

  def get_with_streaming(self, path, **kwargs):
    response = self._get(path, stream=True, **kwargs)

    # Only the dataType endpoint requires streaming.
    for line in response.iter_lines():
      if line:
        try:
          # For all dataType streams.
          decoded_line = json.loads(line.decode('utf-8'))
          try:
            # `transactionalData` is only available on `emailSend`
            # but I don't know how to modify the dict after it's been
            # `yield`ed.
            decoded_line["transactionalData"] = json.loads(decoded_line["transactionalData"])
          except KeyError:
            pass
          yield decoded_line
        except ValueError:
          # For `list_users` streams if json cannot decode.
          yield line.decode('utf-8')

  # 
  # Methods to retrieve data per stream/resource.
  # 

  def lists(self, column_name=None, bookmark=None):
    res = self.get("lists")
    for l in res["lists"]:
      l["createdAt"] = self._epoch_to_datetime_string(l["createdAt"])
      yield l


  def list_users(self, column_name=None, bookmark=None):
    # consider using _get_with_streaming for this.
    res = self.get("lists")
    for l in res["lists"]:
      users = self.get_with_streaming("lists/getUsers", listId=l["id"])
      for user in users:
        yield {
          "email": user,
          "listId": l["id"],
          "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
        }


  def campaigns(self, column_name=None, bookmark=None):
    res = self.get("campaigns")
    for c in res["campaigns"]:
      c["updatedAt"] = self._epoch_to_datetime_string(c["updatedAt"])
      c["createdAt"] = self._epoch_to_datetime_string(c["createdAt"])
      try: 
        c["startAt"] = self._epoch_to_datetime_string(c["startAt"])
      except KeyError:
        c["startAt"] = None
      try:
        c["endedAt"] = self._epoch_to_datetime_string(c["endedAt"])
      except KeyError:
        c["endedAt"] = None
      yield c


  def channels(self, column_name, bookmark):
    res = self.get("channels")
    for c in res["channels"]:
      yield c


  def message_types(self, column_name=None, bookmark=None):
    res = self.get("messageTypes")
    for m in res["messageTypes"]:
      yield m


  def templates(self, column_name, bookmark):
    template_types = [
      "Base",
      "Blast",
      "Triggered",
      "Workflow"
    ]
    message_mediums = [
      "Email",
      "Push",
      "InApp",
      "SMS"
    ]
    for template_type in template_types:
      for medium in message_mediums:
        res = self.get("templates", templateTypes=template_type, messageMedium=medium)
        for t in res["templates"]:
          t["updatedAt"] = self._epoch_to_datetime_string(t["updatedAt"])
          t["createdAt"] = self._epoch_to_datetime_string(t["createdAt"])
          yield t


  def metadata(self, column_name=None, bookmark=None):
    tables = self.get("metadata")
    for t in tables["results"]:
      keys = self.get("metadata/{table_name}".format(table_name=t["name"]))
      for k in keys["results"]:
        value = self.get("metadata/{table_name}/{key}".format(table_name=k["table"], key=k["key"]))
        value["lastModified"] = self._epoch_to_datetime_string(value["lastModified"])
        yield value


  def email_bounce(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="emailBounce", startDateTime=bookmark, endDateTime=endDateTime)


  def email_click(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="emailClick", startDateTime=bookmark, endDateTime=endDateTime)


  def email_complaint(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="emailComplaint", startDateTime=bookmark, endDateTime=endDateTime)


  def email_open(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="emailOpen", startDateTime=bookmark, endDateTime=endDateTime)


  def email_send(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="emailSend", startDateTime=bookmark, endDateTime=endDateTime)


  def email_send_skip(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="emailSendSkip", startDateTime=bookmark, endDateTime=endDateTime)


  def email_subscribe(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="emailSubscribe", startDateTime=bookmark, endDateTime=endDateTime)


  def email_unsubscribe(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="emailUnSubscribe", startDateTime=bookmark, endDateTime=endDateTime)


  def users(self, column_name=None, bookmark=None):
    endDateTime = self._now()
    return self.get_with_streaming("export/data.json", dataTypeName="user", startDateTime=bookmark, endDateTime=endDateTime)





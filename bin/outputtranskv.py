#!/usr/bin/env python

import sys, json, collections, itertools
import splunklib.client as client

from decimal import *
from splunklib.searchcommands import \
    dispatch, ReportingCommand, Configuration, Option, validators


def update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        elif k == "_time":
            d["start_time"] = str(min(Decimal(d.get("start_time",'inf')),Decimal(u[k])))
            if u[k] != d["start_time"]:
                d["end_time"] = str(max(Decimal(d.get("start_time",'-inf')),Decimal(u[k])))
        elif k == "event_count":
            d[k] = d.get(k, 0) + u[k]
        else:
            d[k] = u[k]
    return d

def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk
    
@Configuration(clear_required_fields=True, requires_preop=True)
class outputTransKVCommand(ReportingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    """
    
    transaction_id = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** field to be used as unique identifier for the transaction''',
        require=True, validate=validators.Fieldname())

    testmode = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** set **testmode** to true if the results should not be written to the kv store. default is **false**''',
        require=False, default=False, validate=validators.Boolean())

    collections_data_endpoint = 'storage/collections/data/'
    

    def reduce(self, events):
        # initialize an app service to communicate via REST
        app_service = client.Service(token=self.input_header["sessionKey"])
        output_array = []
        kv_store = self.fieldnames[0]

        # create empty result dictionary
        result_dict = dict()
        # sort events by _time ascending to update the result_dict with the latest data at last
        sorted_events = sorted(events, key=lambda k: k['_time'])
        # loop through events and update the result dictionary
        for event in sorted_events:
            event["event_count"] = 1
            event_as_dict = {event[self.transaction_id]: event}
            update(result_dict, event_as_dict)
        
        response = app_service.request(
            self.collections_data_endpoint + kv_store,
            method = 'get',
            headers = [('content-type', 'application/json')],
            owner = 'nobody',
            app = 'SA-transactional_kv'
        )
        
        if response.status != 200:
            raise Exception, "%d (%s)" % (response.status, response.reason)
        
        body = response.body.read()
        data = json.loads(body)
        
        for item in data:
            current_key = item.get(self.transaction_id)
            if current_key:
                update_record = result_dict.get(current_key)
                if update_record:
                    update(item, update_record)
                    del result_dict[current_key]
                    output_array.append(item)
                    yield item

        # output the values of the result dictionary
        for k, v in result_dict.iteritems():
            output_array.append(v)
            yield v

        # check if the output_array actually contains elements before pushing it into the KV store
        if output_array and not self.testmode:
            for group in grouper(1000, output_array):
                response = app_service.request(
                    self.collections_data_endpoint + kv_store + "/batch_save",
                    method = 'post',
                    headers = [('content-type', 'application/json')],
                    body = json.dumps(group),
                    owner = 'nobody',
                    app = 'SA-transactional_kv'
                )

                if response.status != 200:
                    raise Exception, "%d (%s)" % (response.status, response.reason)

            
dispatch(outputTransKVCommand, sys.argv, sys.stdin, sys.stdout, __name__)

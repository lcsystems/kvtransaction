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
            d["_time"] = str(min(Decimal(d.get("_time",'inf')),Decimal(u[k])))
            duration = Decimal(u[k]) - Decimal(d.get("_time"))
            d["duration"] = str(max(Decimal(d.get("duration",'-inf')),duration))
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
class kvtransactionCommand(ReportingCommand):
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
        cnt_loop = 0

        # create empty result dictionary
        result_dict = dict()
        # sort events by _time ascending to update the result_dict with the latest data at last
        sorted_events = sorted(events, key=lambda k: k['_time'])
        # loop through events and update the result dictionary
        for event in sorted_events:
            cnt_loop += 1
            self.logger.debug("Count sorted events loop: %s" % cnt_loop)
            event["event_count"] = 1
            self.logger.debug("New event: %s" % event)
            if result_dict.get(event[self.transaction_id]):
                update(result_dict[event[self.transaction_id]], event)
            else:
                result_dict[event[self.transaction_id]] = event   
            self.logger.debug("New result_dict entry: %s" % result_dict[event[self.transaction_id]])
        
        response = app_service.request(
            self.collections_data_endpoint + kv_store,
            method = 'get',
            headers = [('content-type', 'application/json')],
            owner = 'nobody',
            app = 'SA-kvtransaction'
        )
        
        if response.status != 200:
            raise Exception, "%d (%s)" % (response.status, response.reason)
        
        body = response.body.read()
        data = json.loads(body)
        
        cnt_loop = 0
        for item in data:
            cnt_loop += 1
            self.logger.debug("Count kv store loop: %s" % cnt_loop)
            current_key = item.get(self.transaction_id)
            if current_key:
                update_record = result_dict.get(current_key)
                if update_record:
                    update(item, update_record)
                    del result_dict[current_key]
                    output_array.append(item)
                    yield item

        cnt_loop = 0
        # output the values of the result dictionary
        for k, v in result_dict.iteritems():
            cnt_loop += 1
            self.logger.debug("Count output loop: %s" % cnt_loop)
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
                    app = 'SA-kvtransaction'
                )

                if response.status != 200:
                    raise Exception, "%d (%s)" % (response.status, response.reason)

            
dispatch(kvtransactionCommand, sys.argv, sys.stdin, sys.stdout, __name__)

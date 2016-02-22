#!/usr/bin/env python

import sys, json, collections, itertools, urllib
import splunk.rest as rest

from decimal import *
from splunklib.searchcommands import \
    dispatch, StreamingCommand, Configuration, Option, validators

def update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        elif k == "_time":
            # update _time with the earliest timestamp found by transaction_id
            d["_time"] = str(min(Decimal(d.get("_time",'inf')),Decimal(u[k])))
            # recalculate duration based on new _time
            duration = Decimal(u[k]) - Decimal(d.get("_time"))
            d["duration"] = str(max(Decimal(d.get("duration",'-inf')),duration))
        elif k == "event_count":
            d[k] = d.get(k, 0) + u[k]
        elif u[k] != '':
            d[k] = u[k]
    return d

def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk
    
@Configuration()
class kvtransaction(StreamingCommand):
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

    collection = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** set **testmode** to true if the results should not be written to the kv store. default is **false**''',
        require=True)

    collections_data_endpoint = 'storage/collections/data/'
    

    def stream(self, events):
        # initialize an app service to communicate via REST
        sessionKey = self.input_header["sessionKey"]
        self.logger.debug("sessionKey is '%s'" % sessionKey)
        output_array = []
        trans_id_list = []
        
        for event in events:
            trans_id_list.append({self.transaction_id: event[self.transaction_id]})
            output_array.append(event)

        # make the trans_id_list unique
        trans_id_list = {v[self.transaction_id]:v for v in trans_id_list}.values()
        
        if len(trans_id_list) > 0:
            # create filter query for requesting kv_store entries
            query = {"$or": trans_id_list}
            # retrieve results from kv store
            self.logger.debug("Filter for transaction ids: %s" % query)
            uri = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
            self.logger.debug("Retrieving transactions from kv store")
            serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
            self.logger.debug("Got rest API response with status %s" % serverResponse['status'])
            kvtrans = json.loads(serverContent)
            self.logger.debug("Converted response to JSON object")

            # convert list of dict to dict of dict
            kvtrans_dict = {item[self.transaction_id]:item for item in kvtrans}
            # self.logger.debug("Converted list to dict: %s" % kvtrans_dict)
            for event in output_array:
                if kvtrans_dict.get(event[self.transaction_id]):
                    self.logger.debug("Found existing transaction with id: %s" % event[self.transaction_id])
                else:
                    self.logger.debug("New transaction with id: %s" % event[self.transaction_id])
                    self.logger.debug("current kv content: %s" % kvtrans_dict)
                kvevent = kvtrans_dict.get(event[self.transaction_id], event)
                old_time = Decimal(kvevent.get('_time','inf'))
                new_time = min(old_time,Decimal(event['_time']))
                old_duration = Decimal(kvevent.get('duration','0'))
                new_duration = max(old_time,Decimal(event['_time']) - new_time)
                new_event_cnt = kvevent.get('event_count',0) + 1
                event['start_time'] = str(new_time)
                event['duration'] = str(new_duration)
                event['event_count'] = new_event_cnt
                yield event
                # update event _time field for storing latest data into kvstore
                event['_time'] = str(new_time)
                event['_key'] = event[self.transaction_id]
                kvtrans_dict[event[self.transaction_id]] = event
                
        # check if the output_array actually contains elements before pushing it into the KV store
        if output_array and not self.testmode:
            for group in grouper(1000, output_array):
                entries = json.dumps(group)
                uri = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s/batch_save' % self.collection
                serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                response = json.loads(serverContent)
                #return response["_key"]
            
dispatch(kvtransaction, sys.argv, sys.stdin, sys.stdout, __name__)

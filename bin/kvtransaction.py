#!/usr/bin/env python

import sys, json, collections, itertools, re, base64
import urllib, hashlib
import splunk.rest as rest

from decimal import *
from splunklib.searchcommands import \
    dispatch, StreamingCommand, Configuration, Option, validators

    
## Supporting function: Treats object "iterable" as iterable tupel
#
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

    ## Syntax
    #
    %(syntax)

    ## Description
    #
    %(description)

    """
    
    transaction_id = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** Field to be used as unique identifier for the transaction.''',
        require=True, validate=validators.Fieldname())

    testmode = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** Set **testmode** to true if the results should not be written to the KV store. Default is **false**.''',
        require=False, default=False, validate=validators.Boolean())

    collection = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** Set **collection** to the KV store to use.''',
        require=True)

    mvlist = Option(
        doc='''
        **Syntax:** **value=***<bool | list>*
        **Description:** Set **mvlist** to true if all fields should be treated as mvlists. 
          Set to a comma-separated list of fields to specify a subset of fields to be treated as mvlists. Default is **false**.''',
        require=False, default=False, validate=validators.BoolOrList())

    mvdedup = Option(
        doc='''
        **Syntax:** **value=***<bool>*
        **Description:** If **mvlist** is set, setting **mvdedup** to **true** deduplicates values in the specified mvlist fields.
          Note that values already written to a KV store will get deduplicated too.''',
        require=False, default=False, validate=validators.Boolean())
        
    collections_data_endpoint = 'storage/collections/data/'

    truth_values = {
        '1': True, '0': False,
        't': True, 'f': False,
        'true': True, 'false': False,
        'y': True, 'n': False,
        'yes': True, 'no': False
    }

    def stream(self, events):
        ## Initialize an app service to communicate via REST
        #
        sessionKey    = self.input_header["sessionKey"]
        output_array  = []
        trans_id_list = []

        ## Get transaction IDs from returned events
        #        
        for event in events:
            trans_id_list.append({self.transaction_id: event[self.transaction_id]})
            output_array.append(event)

        ## Make items in "trans_id_list" unique
        #
        trans_id_list = {v[self.transaction_id]:v for v in trans_id_list}.values()
        
        if len(trans_id_list) > 0:
            ## Create filter query for requesting KV store entries
            #
            query = {"$or": trans_id_list}
            
            ## Retrieve transactions for current transaction IDs from KV store
            #
            self.logger.debug("Filter for transaction ids: %s." % query)
            uri = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
            self.logger.debug("Retrieving transactions from kv store.")
            serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
            self.logger.debug("Got REST API response with status %s." % serverResponse['status'])
            kvtrans = json.loads(serverContent)
            self.logger.debug("Converted response to JSON object.")

            ## Convert list of dict to dict of dict
            #
            kvtrans_dict = {item[self.transaction_id]:item for item in kvtrans}
            #self.logger.debug("Converted list to dict: %s." % kvtrans_dict)
            
            for id in trans_id_list:
                current_event_count = 0
                current_max_time    = 0
                current_min_time    = 99999999999999999
                current_duration    = 0
                
                for event in output_array:
                    if event[self.transaction_id] == id[self.transaction_id]:                  
                        ## Get corresponding KV store entry
                        #            
                        kvevent = kvtrans_dict.get(event[self.transaction_id], {})

                        ## Generate event hash
                        ## Only handle events that didn't already contribute to the transaction from this point onwards
                        #
                        event['hashes'] = str(hashlib.md5(json.dumps(event)).hexdigest())
                        contributed     = False
                        kvfield         = kvevent.get('hashes', [])
                        #self.logger.debug("Event hash is %s." % event['hashes'])
                        if not isinstance(kvfield, list):
                            kvfield = [kvfield]
                        for hash in kvfield:
                            ## Determine if event has already contributed
                            #
                            if event['hashes'] == hash:
                                contributed = True
                                break
                        ## If event has already contributed to the current transaction
                        ## Skip any further processing
                        #
                        if contributed:
                            event['start_time']         = str(kvevent.get('start_time'))
                            event['duration']           = str(kvevent.get('duration'))
                            event['_time']              = str(kvevent.get('_time'))
                            event['event_count']        = int(kvevent.get('event_count',0)) - 1
                            event['_key']               = event[self.transaction_id]
                            kvtrans_dict[event['_key']] = event
                            continue
                        ## Else handle event as usual
                        #
                        else:
                            kvfield.append(event['hashes'])
                            event['hashes'] = kvfield
                            
                        ## Define mvlist behavior
                        #
                        if self.mvlist in kvtransaction.truth_values:
                            self.mvlist = kvtransaction.truth_values[self.mvlist]
                        self.logger.debug("Parameter mvlist set to %s." % self.mvlist)

                        if not isinstance(self.mvlist, bool):
                            iter_list = [i.strip() for i in self.mvlist.split(',')]
                            self.logger.debug("Parameter iter_list set to %s." % iter_list)
                            
                            ## Check if iter_list contains invalid fieldnames
                            #
                            for i in iter_list:
                                contained = False
                                for j in event.keys():
                                    if i == j:
                                        contained = True
                                ## TODO: This might have to be removed if events of different formats with different contents shall be aggregated
                                #
                                if not contained:
                                    raise ValueError('The argument "mvlist" contains non-existent fieldnames: {}'.format(i))
                            if self.fieldnames:
                                for i in iter_list:
                                    contained = False
                                    for j in self.fieldnames:
                                        if i == j:
                                            contained = True
                                    if not contained:
                                        ## Adjust iter_list to fieldnames
                                        #
                                        iter_list.remove(i)
                            
                            ## Process fields (determine new field values for KV store entries)
                            #                        
                            self.logger.debug("Iterating over fields: %s." % iter_list)
                            for field in iter_list:
                                ## Do not treat transaction ID and time as mv fields
                                #
                                if field == self.transaction_id or field == '_time':
                                    self.logger.debug("Do NOT generate multi valued fields for '%s'." % field)
                                    continue
                                else:
                                    kvfield = kvevent.get(field, [])
                                    self.logger.debug("Current event field '%s' with value '%s'." % (field, event[field]))
                                    self.logger.debug("Current KV field '%s' with value '%s'." % (field, kvfield))
                                    self.logger.debug("Current type of KV field '%s' is '%s'." % (field, type(kvfield)))
                                    if not isinstance(kvfield, list):
                                        kvfield = [kvfield]
                                    kvfield.append(event[field])
                                    ## Control deduplication
                                    #
                                    if self.mvdedup:
                                        kvfield = list(set(kvfield))
                                    event[field] = kvfield
                                    self.logger.debug("New event field '%s' with value '%s'." % (field, event[field]))
                        
                        ## Handle boolean definitions
                        #
                        elif self.mvlist:
                            if self.fieldnames:
                                iter_list = self.fieldnames
                            else:
                                iter_list = list(event.keys())
                            ## Process fields (determine new field values for KV store entries)
                            #                        
                            for field in iter_list:
                                ## Do not treat transaction ID and time as mv fields
                                #
                                if field == self.transaction_id or field == '_time':
                                    continue
                                else:
                                    kvfield = kvevent.get(field, [])
                                    if not isinstance(kvfield, list):
                                        kvfield = [kvfield]
                                    kvfield.append(event[field])
                                    ## Control deduplication
                                    #
                                    if self.mvdedup:
                                        kvfield = list(set(kvfield))
                                    event[field] = kvfield
     
                        ## Calculate new transaction properties
                        #
                        event['event_count']        = int(kvevent.get('event_count',0)) + 1
                        event['_key']               = event[self.transaction_id]
                        kvtrans_dict[event['_key']] = event

                        if event['event_count'] > current_event_count:
                            current_event_count = event['event_count']
                        if Decimal(event['_time']) > Decimal(current_max_time):
                            current_max_time = Decimal(event['_time'])
                        if Decimal(event['_time']) < Decimal(current_min_time):
                            current_min_time = Decimal(event['_time'])
                        current_duration            = max(Decimal(kvevent.get('duration','0')), current_max_time - current_min_time)
                        event['start_time']         = str(current_min_time)
                        event['duration']           = str(current_duration)
                        event['event_count']        = current_event_count
                        event['_time']              = str(current_min_time)
                        event['_key']               = event[self.transaction_id]
                        kvtrans_dict[event['_key']] = event
                 
                 
                ## Yield the correct events for each transaction_id
                #
                for event in output_array:
                    if not self.mvlist and event[self.transaction_id] == id[self.transaction_id]:
                        yield event
                    elif event[self.transaction_id] == id[self.transaction_id] and event['event_count'] == current_event_count:
                        yield event
          
          
        ## Check if "output_array" actually contains elements before pushing into KV store
        #
        if output_array and not self.testmode:
            ## Push piecemeal in 1000 item chunks
            #
            for group in grouper(1000, output_array):
                entries                       = json.dumps(group)
                uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s/batch_save' % self.collection
                serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                response                      = json.loads(serverContent)
            
dispatch(kvtransaction, sys.argv, sys.stdin, sys.stdout, __name__)

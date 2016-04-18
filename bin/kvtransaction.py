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
        sessionKey = self.input_header['sessionKey']
        event_list = []
        id_list    = []

        ## Get aggregate events and distinct transaction IDs
        #        
        for event in events:
            id_list.append({self.transaction_id: event[self.transaction_id]})
            event_list.append(event)
        id_list = {v[self.transaction_id]:v for v in id_list}.values()

        if len(id_list) > 0:
            ## Create filter query for requesting KV store entries
            #
            query = {"$or": id_list}
            
            ## Retrieve transactions for current transaction IDs from KV store
            #
            #self.logger.debug("Filter for transaction ids: %s." % query)
            uri = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
            #self.logger.debug("Retrieving transactions from kv store.")
            serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
            #self.logger.debug("Got REST API response with status %s." % serverResponse['status'])
            kvtransactions = json.loads(serverContent)
            #self.logger.debug("Converted response to JSON object.")

            ## Convert list of dict to dict of dict
            #
            transaction_dict = {item[self.transaction_id]:item for item in kvtransactions}
            #self.logger.debug("Currently stored transactions: %s." % transaction_dict)
            
            for id in id_list:
                event_count = 0

                for event in event_list:
                    if event[self.transaction_id] == id[self.transaction_id]:
                        ## Get KV store entry correspondig with transaction_id as OrderedDict
                        #
                        kvevent = transaction_dict.get(event[self.transaction_id], {})
                        #self.logger.debug("Corresponding KV event: %s." % kvevent)
                        
                        ## Generate event checksum
                        ## Only handle events that didn't already contribute to the transaction from this point onwards
                        #
                        #event['_time'] = str(Decimal(event['_time']))
                        hashable_event = event
                        try:
                            del hashable_event['_key']
                            del hashable_event['event_count']
                            del hashable_event['duration']
                            del hashable_event['start_time']
                            del hashable_event['hashes']
                        except:
                            pass
                        
                        #self.logger.debug("JSON event: %s." % json.dumps(hashable_event))
                        event['hashes'] = str(hashlib.md5(json.dumps(hashable_event)).hexdigest())
                        contributed     = False
                        kvhashes        = kvevent.get('hashes', [])
                        #self.logger.debug("Event hash is %s." % event['hashes'])
                        #self.logger.debug("Event is %s." % event)
                        if not isinstance(kvhashes, list):
                            kvhashes = [kvhashes]
                        for hash in kvhashes:
                            ## Determine if event has already contributed
                            #
                            if event['hashes'] == hash:
                                contributed = True
                                break
                        ## If event has already contributed to the current transaction
                        ## Skip any further processing
                        #
                        if contributed:
                            #event['start_time']         = str(kvevent.get('start_time'))
                            #event['duration']           = str(kvevent.get('duration'))
                            #event['_time']              = str(kvevent.get('_time'))
                            #event['event_count']        = int(kvevent.get('event_count',0))
                            #event['_key']               = event[self.transaction_id]
                            #event_count                 = event['event_count']
                            #transaction_dict[event['_key']] = event
                            #event_list.remove(event)
                            event_list = [events for events in event_list if not event]
                            #self.logger.debug("Skipped processing for event %s." % event)
                            #event = kvevent
                            continue
                        ## Else handle event as usual
                        #
                        else:
                            kvhashes.append(event['hashes'])
                            event['hashes'] = kvhashes

                        ## Define mvlist behavior
                        #
                        if self.mvlist in kvtransaction.truth_values:
                            self.mvlist = kvtransaction.truth_values[self.mvlist]
                        #self.logger.debug("Parameter mvlist set to %s." % self.mvlist)

                        if not isinstance(self.mvlist, bool):
                            iter_list = [i.strip() for i in self.mvlist.split(',')]
                            #self.logger.debug("Parameter iter_list set to %s." % iter_list)
                            
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
                            #self.logger.debug("Iterating over fields: %s." % iter_list)
                            for field in iter_list:
                                ## Do not treat transaction ID and time as mv fields
                                #
                                if field == self.transaction_id or field == '_time' or field == 'hashes':
                                    #self.logger.debug("Do NOT generate multi valued fields for '%s'." % field)
                                    continue
                                else:
                                    kvfield = kvevent.get(field, [])
                                    #self.logger.debug("Current event field '%s' with value '%s'." % (field, event[field]))
                                    #self.logger.debug("Current KV field '%s' with value '%s'." % (field, kvfield))
                                    #self.logger.debug("Current type of KV field '%s' is '%s'." % (field, type(kvfield)))
                                    if not isinstance(kvfield, list):
                                        kvfield = [kvfield]
                                    kvfield.append(event[field])
                                    ## Control deduplication
                                    #
                                    if self.mvdedup:
                                        kvfield = list(set(kvfield))
                                    event[field] = kvfield
                                    #self.logger.debug("New event field '%s' with value '%s'." % (field, event[field]))
                        
                        ## Handle boolean definitions
                        #
                        elif self.mvlist:
                            if self.fieldnames:
                                iter_list = self.fieldnames
                            else:
                                iter_list = list(event.keys())
                            ## Process fields (determine new field values for KV store entries)
                            #
                            #self.logger.debug("Iterating over fields: %s." % iter_list)                            
                            for field in iter_list:
                                ## Do not treat transaction ID and time as mv fields
                                #
                                if field == self.transaction_id or field == '_time' or field == 'hashes':
                                    continue
                                else:
                                    kvfield = kvevent.get(field, [])
                                    #self.logger.debug("Current event field '%s' with value '%s'." % (field, event[field]))
                                    #self.logger.debug("Current KV field '%s' with value '%s'." % (field, kvfield))
                                    #self.logger.debug("Current type of KV field '%s' is '%s'." % (field, type(kvfield)))                                    
                                    if not isinstance(kvfield, list):
                                        kvfield = [kvfield]
                                    kvfield.append(event[field])
                                    ## Control deduplication
                                    #
                                    if self.mvdedup:
                                        kvfield = list(set(kvfield))
                                    event[field] = kvfield

                        elif not self.mvlist:
                            pass
                            
                        ## Calculate new transaction properties
                        #
                        #event['_key']               = event[self.transaction_id]
                        #transaction_dict[event['_key']] = event

                        current_min_time                = min(Decimal(kvevent.get('_time','inf')), Decimal(event['_time']))
                        current_max_time                = max(Decimal(kvevent.get('_time',0)) + Decimal(kvevent.get('duration',0)), Decimal(event['_time']))
                        event_count                     = int(kvevent.get('event_count',0)) + 1
                        #current_duration                = max(Decimal(kvevent.get('duration','0')), current_max_time - current_min_time)
                        event['start_time']             = str(current_min_time)
                        event['duration']               = str(current_max_time - current_min_time)
                        event['event_count']            = event_count
                        event['_time']                  = str(current_min_time)
                        event['_key']                   = event[self.transaction_id]
                        transaction_dict[event['_key']] = event
                        
                 
                ## Yield the correct events for each transaction_id
                #
                #self.logger.debug("Event List %s." % event_list)
                for event in event_list:
                    if not self.mvlist and event[self.transaction_id] == id[self.transaction_id]:
                        yield event
                    elif self.mvlist and event[self.transaction_id] == id[self.transaction_id] and int(event['event_count']) == event_count:
                        yield event
          
          
        ## Check if "event_list" actually contains elements before pushing into KV store
        #
        if event_list and not self.testmode:
            ## Push piecemeal in 1000 item chunks
            #
            for group in grouper(1000, event_list):
                entries                       = json.dumps(group)
                uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s/batch_save' % self.collection
                rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                #serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                #response                      = json.loads(serverContent)

dispatch(kvtransaction, sys.argv, sys.stdin, sys.stdout, __name__)

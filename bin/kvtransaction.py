#!/usr/bin/env python

import sys, json, collections, itertools, time
import urllib, hashlib
import splunklib.client as client
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

       
def get_kv_entries(app, collection, sessionKey, transaction_id, id_list):
    query                         = {"$or": id_list}
    uri                           = '/servicesNS/nobody/%s/storage/collections/data/%s?query=%s' % (app, collection, urllib.quote(json.dumps(query)))
    serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
    try:
        kvtransactions        = json.loads(serverContent)
    except:
        raise ValueError("REST call returned invalid response. Presumably an invalid collection was provided: %s. Details: %s" % (collection, serverContent))
    return {item[transaction_id]:collections.OrderedDict(item) for item in kvtransactions}

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
        
    app = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** Set **app** to the app containing the KV store definition.''',
        require=False, default="SA-kvtransaction")

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



    def stream(self, events):
        """                                             """
        """   Initialize event independent variables.   """
        """                                             """
        
        event_list       = []
        results_list     = []
        id_list          = []
        field_list       = []
        key_list         = set()
        transaction_dict = {}

        sessionKey = self.metadata.searchinfo.session_key
        #self.logger.debug("Session Key2: %s" % str(sessionKey))
        
        
        if self.testmode:
            self.logger.info("Test mode is activated. Will not write results to KV store")

        ## Aggregate events, distinct fieldnames and distinct transaction IDs
        ## Calculate event checksums while doing so
        #
        self.logger.info("Starting to preprocess incoming events.")
        for event in events:
            hashable_event = event
            try:
                del hashable_event['_key']
                del hashable_event['event_count']
                del hashable_event['duration']
                del hashable_event['start_time']
                del hashable_event['_hashes']
                
                for key in hashable_event:
                    if '__latest_' in key:
                        del hashable_event[key]
            except:
                pass
            event['_hashes'] = str(hashlib.md5(json.dumps(hashable_event)).hexdigest())

            try:
                id_list.append({self.transaction_id: event[self.transaction_id]})
            except KeyError:
                pass
            event_list.append(event)

            for key in event.keys():
                key_list.add(key)

        id_list  = {v[self.transaction_id]:v for v in id_list}.values()
        key_list = list(key_list)
        self.logger.info("Finished preprocessing %s incoming events with %s unique transaction ids" % (len(event_list), len(id_list)))

        ## Set mvlist behavior for all relevant fields
        #
        if isinstance(self.mvlist, bool):
            if self.fieldnames:
                field_list = self.fieldnames
            else:
                field_list = list(key_list)

        elif not isinstance(self.mvlist, bool):
            field_list = [i.strip() for i in self.mvlist.split(',')]

            ## Adjust field_list to actually available fieldnames
            #
            for i in field_list:
                contained = False
                
                if i in key_list:
                    contained = True
                    
                if not contained:
                    field_list = [item for item in field_list if not i]
                else:
                    pass            


        """                                                             """
        """ Initialize event specific variables.                        """
        """ Process events and calculate new transaction properties.    """
        """                                                             """

        if len(id_list) > 0:
            ## Set maximum length for kv store requests
            #
            self.logger.info("Retrieving relevant stored transactions.")
            max_query_len = 2000 - len(self.collection)
        
            ## Directly request if limit is not reached
            #
            if len(str(id_list)) < max_query_len:
                transaction_dict = get_kv_entries(self.app, self.collection, sessionKey, self.transaction_id, id_list)
            ## Else request in appropriate chunks
            #
            else:
                len_list   = [len(str(id[self.transaction_id])) for id in id_list]
                length     = 0
                last_index = 0

                for index, element in enumerate(len_list):
                    if max_query_len < length + element:
                        transaction_dict = get_kv_entries(self.app, self.collection, sessionKey, self.transaction_id, id_list[last_index:index])                        
                        length           = element
                        last_index       = index
                    else:
                        length     = length + element
                    ## Trigger condition for the last chunk, since it might never reach the size neccessary for triggering a request otherwise
                    #
                    if index == len(len_list):
                        transaction_dict.update(get_kv_entries(self.app, self.collection, sessionKey, self.transaction_id, id_list[last_index:index]))
            self.logger.info("Retrieved %s stored transactions." % (len(transaction_dict)))

            ## Process events
            #
            self.logger.info("Start processing events")
            for event in event_list:
                ## Buffer KV store entry (transaction) correspondig with the current event as orderedDict
                #
                kvevent     = transaction_dict.get(event[self.transaction_id], {})
                kv_max_time = Decimal(kvevent.get('_time',0)) + Decimal(kvevent.get('duration',0))
                try:
                    event_time = Decimal(event['_time'])
                except KeyError:
                    event_time = Decimal(time.time()).quantize(Decimal('.000001'), rounding=ROUND_DOWN)
                #self.logger.debug("Corresponding KV event: %s." % kvevent)

                
                ## Check if the event already contributed to the transaction
                ## If so, skip further processing entirely
                #
                kvhashes = kvevent.get('_hashes', [])
                if not isinstance(kvhashes, list):
                    kvhashes = [kvhashes]
                if event['_hashes'] in kvhashes:
                    #self.logger.debug("Skipped processing for event with ID %s." % event[self.transaction_id])
                    continue
                else:
                    kvhashes.append(event['_hashes'])
                    event['_hashes'] = kvhashes


                ## Determine the transaction's new field values
                #
                if self.mvlist or not isinstance(self.mvlist, bool):                                           
                    for field in field_list:
                        if field == self.transaction_id or field == '_time' or field == '_hashes' or '__latest_' in field:
                            continue
                        else:
                            kvfield      = kvevent.get(field, [])
                            latest_field = "__latest_%s" % field
                            field_value  = event.get(field, "")
                            if not isinstance(kvfield, list):
                                kvfield = [kvfield]
                            if field_value and len(field_value) > 0:
                                kvfield.append(field_value)
                                if event_time > kv_max_time or kv_max_time == '0' or kvevent.get(latest_field, "") == '':
                                    event[latest_field] = field_value
                                else:
                                    event[latest_field] = kvevent.get(latest_field, "")
                            else:
                                event[latest_field] = kvevent.get(latest_field, "")
                                
                            ## Control deduplication
                            #
                            if self.mvdedup:
                                kvfield = list(set(kvfield))
                            event[field] = kvfield

                if not self.mvlist:
                    for field in field_list:
                        if field == self.transaction_id or field == '_time' or field == '_hashes' or '__latest_' in field:
                            continue
                        else:
                            kvfield      = kvevent.get(field, [])
                            latest_field = "__latest_%s" % field
                            field_value  = event.get(field, "")
                            if field_value and len(field_value) > 0:
                                if event_time > kv_max_time or kv_max_time == '0' or kvevent.get(latest_field, "") == '':
                                    kvfield             = field_value
                                    event[latest_field] = field_value
                                else:
                                    kvfield             = kvevent.get(latest_field, "")
                                    event[latest_field] = kvevent.get(latest_field, "")
                            #else:
                            #    kvfield             = kvevent.get(latest_field, "")
                            #    event[latest_field] = kvevent.get(latest_field, "")
                            event[field] = kvfield


                ## Calculate the transaction's new properties
                #
                for kvfield in kvevent:
                    if not kvfield in field_list:
                        event.update({kvfield:kvevent[kvfield]})
                current_min_time                = min(Decimal(kvevent.get('_time','inf')), event_time)
                current_max_time                = max(kv_max_time, event_time)
                event_count                     = int(kvevent.get('event_count',0)) + 1
                event['start_time']             = str(current_min_time)
                event['duration']               = str(current_max_time - current_min_time)
                event['event_count']            = event_count
                event['_time']                  = str(current_min_time)
                event['_key']                   = event[self.transaction_id]
                transaction_dict[event['_key']] = event

            ## Yield the correct events for each transaction ID
            #
            self.logger.info("Displaying results in splunk")
            for transaction in transaction_dict:
                event = transaction_dict.get(transaction, {})
                yield event
                results_list.append(event)
                
        ## Push into KV store piecemeal in 1000 item chunks, if transactions got updated or created
        #
        if results_list and not self.testmode:
            self.logger.info("Saving results to kv store")
            for group in grouper(1000, results_list):
                entries                       = json.dumps(group, sort_keys=True)
                uri                           = '/servicesNS/nobody/%s/storage/collections/data/%s/batch_save' % (self.app, self.collection)
                rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)

dispatch(kvtransaction, sys.argv, sys.stdin, sys.stdout, __name__)

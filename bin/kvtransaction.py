#!/usr/bin/env python

import sys, json, collections, itertools, re, base64
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
        
        sessionKey       = re.search('u\'session_key\'\:\s+u\'([^\']+)', str(self.metadata)).group(1)
        #self.logger.debug("Session Key2: %s" % str(sessionKey))
        
        
        if self.testmode:
            self.logger.info("Test mode is activated. Will not write results to KV store")

        ## Aggregate events, distinct fieldnames and distinct transaction IDs
        ## Calculate event checksums while doing so
        #
        self.logger.info("Starting to preprocess incoming events.")
        event_cnt = 0
        for event in events:
            event_cnt += 1
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

            id_list.append({self.transaction_id: event[self.transaction_id]})
            event_list.append(event)

            for key in event.keys():
                key_list.add(key)

        id_list  = {v[self.transaction_id]:v for v in id_list}.values()
        key_list = list(key_list)
        self.logger.info("Finished preprocessing %s incoming events with %s unique transaction ids" % (event_cnt, len(id_list)))

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
            ## Original request mechanism:
            #query                         = {"$or": id_list}
            #uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
            #serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
            #kvtransactions                = json.loads(serverContent)
            #transaction_dict              = {item[self.transaction_id]:collections.OrderedDict(item) for item in kvtransactions}
            
            
            ## The original request mechanism yields HTTP 414 errors for too many transaction IDs.
            ## Exceptions occurs at a URL length of around 354000 chars. Maximum URL length for most browsers is 2000 chars.
            ## Thresholds up to around 70000 seem to work fine.
            ## TODO: Which threshold is correct and why?
            #
            ## The following piece of code works reliably using usual browsers threshold but is performing bad.
            #
            """
            max_query_len = 1673 - len(self.collection)
            query_list    = []
            
            self.logger.info("Retriving transactions already stored in KV store")
            if len(str(id_list)) > max_query_len:
                for id in id_list:
                    if len(str(query_list)) + len(str(id)) < max_query_len:
                        query_list.append(id)
                    else:
                        query                         = {"$or": query_list}
                        uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
                        serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
                        kvtransactions                = json.loads(serverContent)
                        transaction_dict              = {item[self.transaction_id]:collections.OrderedDict(item) for item in kvtransactions}
                        del query_list[:]
                        query_list.append(id)
                        query.clear()
            else:
                query                         = {"$or": id_list}
                uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
                serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
                kvtransactions                = json.loads(serverContent)
                transaction_dict              = {item[self.transaction_id]:collections.OrderedDict(item) for item in kvtransactions}     
            """

            
            ## So far this is the best performing and equally reliable working pice of code.
            ## However, where do these threshold values originate from?
            #
            max_query_len = 70000 - len(self.collection)
            query_list    = []
            
            if len(str(id_list)) > max_query_len:
                for id in range(0,len(id_list),50000):
                    if len(str(query_list)) + len(str(id)) < max_query_len:
                        query_list.append(id_list[id:id+50000])
                    else:
                        query                         = {"$or": query_list}
                        uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
                        serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
                        try:
                            kvtransactions            = json.loads(serverContent)
                        except:
                            raise ValueError("REST call returned invalid response. Presumably an invalid collection was provided: %s" % self.collection)
                        transaction_dict              = {item[self.transaction_id]:collections.OrderedDict(item) for item in kvtransactions}
                        del query_list[:]
                        query_list.append(id_list[id:id+50000])
                        query.clear()
            else:
                query                         = {"$or": id_list}
                uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
                serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
                try:
                    kvtransactions                = json.loads(serverContent)
                except:
                    raise ValueError("REST call returned invalid response. Presumably an invalid collection was provided: %s" % self.collection)
                transaction_dict              = {item[self.transaction_id]:collections.OrderedDict(item) for item in kvtransactions}
            
            

            ## Process events
            #
            self.logger.info("Start processing events")
            for event in event_list:
                ## Buffer KV store entry (transaction) correspondig with the current event as orderedDict
                #
                kvevent     = transaction_dict.get(event[self.transaction_id], {})
                kv_max_time = Decimal(kvevent.get('_time',0)) + Decimal(kvevent.get('duration',0))
                event_time  = Decimal(event['_time'])
                #self.logger.debug("Corresponding KV event: %s." % kvevent)

                
                ## Check if the event already contributed to the transaction
                ## If so, skip further processing entirely
                #
                contributed = False
                kvhashes    = kvevent.get('_hashes', [])
                if not isinstance(kvhashes, list):
                    kvhashes = [kvhashes]

                for hash in kvhashes:
                    if event['_hashes'] == hash:
                        contributed = True
                        break

                if contributed:
                    #event_list = [events for events in event_list if not event]
                    #self.logger.debug("Skipped processing for event %s." % event)
                    continue
                else:
                    kvhashes.append(event['_hashes'])
                    event['_hashes'] = kvhashes


                ## Determine the transaction's new field values
                #
                if self.mvlist or not isinstance(self.mvlist, bool):                                           
                    for field in field_list:
                        if field == self.transaction_id or field == '_time' or field == '_hashes' or '__latest_' in key:
                            continue
                        else:
                            kvfield      = kvevent.get(field, [])
                            latest_field = "__latest_%s" % field
                            if not isinstance(kvfield, list):
                                kvfield = [kvfield]
                            field_value = event.get(field, "")
                            if field_value and len(field_value) > 0:
                                kvfield.append(field_value)
                                if event_time > kv_max_time:
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
                        if field == self.transaction_id or field == '_time' or field == '_hashes' or '__latest_' in key:
                            continue
                        else:
                            kvfield      = kvevent.get(field, [])
                            latest_field = "__latest_%s" % field
                            field_value  = event.get(field, "")
                            if field_value and len(field_value) > 0:
                                if event_time > kv_max_time:
                                    kvfield             = field_value
                                    event[latest_field] = field_value
                                else:
                                    kvfield             = kvevent.get(latest_field, "")
                                    event[latest_field] = kvevent.get(latest_field, "")
                            else:
                                kvfield             = kvevent.get(latest_field, "")
                                event[latest_field] = kvevent.get(latest_field, "")
                            event[field] = kvfield


                ## Calculate the transaction's new properties
                #
                current_min_time                = min(Decimal(kvevent.get('_time','inf')), event_time)
                current_max_time                = max(kv_max_time, event_time)
                event_count                     = int(kvevent.get('event_count',0)) + 1
                #current_duration                = max(Decimal(kvevent.get('duration','0')), current_max_time - current_min_time)
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
                entries                       = json.dumps(group)
                uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s/batch_save' % self.collection
                rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                response                      = json.loads(serverContent)
                #self.logger.debug(response)

dispatch(kvtransaction, sys.argv, sys.stdin, sys.stdout, __name__)

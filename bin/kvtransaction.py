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


    ## With the current implementation of Search Command Protocol v2 valid login credentials have to be provided at this point.
    ## This necessity will be removed as soon as this requirement is obsolete
    #
    HOST     = "localhost"
    PORT     = 8089
    USERNAME = "admin"
    PASSWORD = "changeme"


    def stream(self, events):
        """                                             """
        """   Initialize event independent variables.   """
        """                                             """
        
        event_list       = []
        results_list     = []
        id_list          = []     # TODO: Migrate to set
        field_list       = []
        key_list         = set()
        transaction_dict = {}
        
        service          = client.connect(host=self.HOST, port=self.PORT, username=self.USERNAME, password=self.PASSWORD)
        sessionKey       = service.request('auth/', method='GET')
        sessionKey       = re.search('splunkd_\d{4,5}=([^\;]+)', str(sessionKey)).group(1)
        #sessionKey      = self.input_header['sessionKey']


        ## Aggregate events, distinct fieldnames and distinct transaction IDs
        ## Calculate event checksums while doing so
        #        
        for event in events:
            hashable_event = event
            try:
                del hashable_event['_key']
                del hashable_event['event_count']
                del hashable_event['duration']
                del hashable_event['start_time']
                del hashable_event['_hashes']
            except:
                pass
            event['_hashes'] = str(hashlib.md5(json.dumps(hashable_event)).hexdigest())

            id_list.append({self.transaction_id: event[self.transaction_id]})
            event_list.append(event)

            for key in event.keys():
                key_list.add(key)

        id_list  = {v[self.transaction_id]:v for v in id_list}.values()
        key_list = list(key_list)


        ## Set mvlist behavior for all relevant fields
        #
        if self.mvlist:
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
        """ Initialze event specific variables.                         """
        """ Process events and calculate new transaction properties.    """
        """                                                             """

        if len(id_list) > 0:
            ## Create filter query for requesting corresponding KV store entries (transactions)
            ## Retrieve these transactions as dict of orderedDict
            #
            query                         = {"$or": id_list}
            #self.logger.debug("Filter for transaction ids: %s." % query)
            uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
            serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
            kvtransactions                = json.loads(serverContent)
            transaction_dict              = {item[self.transaction_id]:item for item in kvtransactions}
            #self.logger.debug("Currently stored transactions: %s." % transaction_dict)


            ## Process events by transaction ID
            #
            for id in id_list:
                event_count = 0
   
                for event in event_list:
                    if event[self.transaction_id] == id[self.transaction_id]:
                        ## Buffer KV store entry (transaction) correspondig with the current event as orderedDict
                        #
                        kvevent = transaction_dict.get(event[self.transaction_id], {})
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
                            event_list = [events for events in event_list if not event]
                            #self.logger.debug("Skipped processing for event %s." % event)
                            continue
                        else:
                            kvhashes.append(event['_hashes'])
                            event['_hashes'] = kvhashes


                        ## Determine the transaction's new field values
                        #
                        if self.mvlist or not isinstance(self.mvlist, bool):                                           
                            for field in field_list:
                                if field == self.transaction_id or field == '_time' or field == '_hashes':
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

                        if not self.mvlist:
                            pass


                        ## Calculate the transaction's new properties
                        #
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
                     

                ## Yield the correct events for each transaction ID
                #
                for event in event_list:
                    if event[self.transaction_id] == id[self.transaction_id] and int(event['event_count']) == event_count:
                        yield event
                        results_list.append(event)

          
        ## Push into KV store piecemeal in 1000 item chunks, if transactions got updated or created
        #
        if results_list and not self.testmode:
            for group in grouper(1000, results_list):
                entries                       = json.dumps(group)
                uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s/batch_save' % self.collection
                rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                #serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                #response                      = json.loads(serverContent)

dispatch(kvtransaction, sys.argv, sys.stdin, sys.stdout, __name__)
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
        """
            Initialize event independent variables.
        """

        ## Initialize an app service to communicate via REST
        #
        sessionKey       = self.input_header['sessionKey']
        event_list       = []
        id_list          = []
        field_list       = []
        key_list         = set()
        transaction_dict = {}


        ## Aggregate events and distinct transaction IDs
        ## Calculate event checksum while doing so
        #        
        for event in events:
            hashable_event = event
            try:
                del hashable_event['_key']
                del hashable_event['event_count']
                del hashable_event['duration']
                del hashable_event['start_time']
                del hashable_event['hashes']
            except:
                pass
            event['hashes'] = str(hashlib.md5(json.dumps(hashable_event)).hexdigest())
            #self.logger.debug("Event hash is %s." % event['hashes'])
            #self.logger.debug("Event is %s." % event)

            id_list.append({self.transaction_id: event[self.transaction_id]})
            event_list.append(event)
            for key in event.keys():
                key_list.add(key)
        id_list = {v[self.transaction_id]:v for v in id_list}.values()
        key_list = list(key_list)


        ## Initialize mvlist behavior
        #
        if self.mvlist:
            if self.fieldnames:
                field_list = self.fieldnames
            else:
                field_list = list(key_list)
                
        elif not isinstance(self.mvlist, bool):
            field_list = [i.strip() for i in self.mvlist.split(',')]
            ## Check if field_list contains invalid fieldnames
            #
            for i in field_list:
                contained = False
                if i in key_list:
                    contained = True

                if self.fieldnames and not contained:
                    ## Adjust
                    #
                    field_list = [item for item in field_list if not i]
                ## TODO: This might have to be removed if events of different formats with different contents shall be aggregated
                #
                elif not self.fieldnames and not contained:
                    self.logger.debug("Value error %s" % i)
                    field_list = [item for item in field_list if not i]
                    #raise ValueError('The argument "mvlist" contains non-existent fieldnames: {}'.format(i))
                else:
                    pass            
        #self.logger.debug("Parameter mvlist set to %s." % self.mvlist)
        #self.logger.debug("Parameter field_list set to %s." % field_list)
        #self.logger.debug("Parameter key_list set to %s." % key_list)

        if len(id_list) > 0:
            ## Create filter query for requesting KV store entries
            #
            query = {"$or": id_list}
            #self.logger.debug("Filter for transaction ids: %s." % query)

            ## Retrieve transactions for current transaction IDs from KV store
            #
            uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(query)))
            serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
            kvtransactions                = json.loads(serverContent)
            transaction_dict              = {item[self.transaction_id]:item for item in kvtransactions}
            #self.logger.debug("Currently stored transactions: %s." % transaction_dict)


            """
                Initialze event specific variables and process events.
            """
            
            #for id in id_list:
                #event_count = 0
                #uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s?query=%s' % (self.collection, urllib.quote(json.dumps(id)))
                #serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
                #kvtransactions                = json.loads(serverContent)
                #transaction_dict              = {item[self.transaction_id]:item for item in kvtransactions}
   
            for event in event_list:
                #if event[self.transaction_id] == id[self.transaction_id]:
                ## Get KV store entry correspondig with transaction_id as OrderedDict
                #
                kvevent = transaction_dict.get(event[self.transaction_id], {})
                #self.logger.debug("Corresponding KV event: %s." % kvevent)


                ## Check if the event already contributed to the transaction
                ## If so, skip further processing entirely
                #
                contributed = False
                kvhashes    = kvevent.get('hashes', [])
                if not isinstance(kvhashes, list):
                    kvhashes = [kvhashes]

                for hash in kvhashes:
                    if event['hashes'] == hash:
                        contributed = True
                        break

                if contributed:
                    event_list = [events for events in event_list if not event]
                    #self.logger.debug("Skipped processing for event %s." % event)
                    continue
                else:
                    kvhashes.append(event['hashes'])
                    event['hashes'] = kvhashes


                if self.mvlist or not isinstance(self.mvlist, bool):                        
                    ## Process fields (determine new field values for KV store entries)
                    #                        
                    self.logger.debug("Iterating over fields: %s." % field_list)
                    for field in field_list:
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
                if not self.mvlist:
                    pass

                ## Calculate new transaction properties
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
      
                 
            #self.logger.debug("Currently stored transactions: %s." % transaction_dict)
            ## Yield the correct events for each transaction_id
            #
            #self.logger.debug("Event List %s." % event_list)
            for event in event_list:
                yield event
                #if not self.mvlist and event[self.transaction_id] == id[self.transaction_id]:
                #    yield event
                #elif self.mvlist and event[self.transaction_id] == id[self.transaction_id] and int(event['event_count']) == event_count:
                #    yield event
                    #event_list = [events for events in event_list if event]
          
        ## Altenative KV store calls
        #
        #event_list = [events for events in event_list if not event]
        #entries = json.dumps(event_list)
        #uri     = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s/batch_save' % self.collection
        #rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
        

          
        ## Check if "event_list" actually contains elements before pushing into KV store
        #
        if event_list and not self.testmode:
            ## Push piecemeal in 1000 item chunks
            #
            for group in grouper(1000, event_list):
                entries                       = json.dumps(group)
                uri                           = '/servicesNS/nobody/SA-kvtransaction/storage/collections/data/%s/batch_save' % self.collection
                rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey, jsonargs=entries)
                response                      = json.loads(serverContent)

dispatch(kvtransaction, sys.argv, sys.stdin, sys.stdout, __name__)
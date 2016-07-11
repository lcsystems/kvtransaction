#!/usr/bin/env python

import sys, json, collections, itertools, urllib, re
import splunk.rest as rest
import splunklib.client as client

from decimal import *
from datetime import timedelta, datetime
from splunklib.searchcommands import \
    dispatch, GeneratingCommand, Configuration, Option, validators

@Configuration()
class outputkvtransaction(GeneratingCommand):
    """ %(synopsis)

    ## Syntax
    #
    %(syntax)

    ## Description
    #
    %(description)

    """

    minevents = Option(
        doc='''
        **Syntax:** **value=***<integer>*
        **Description:** Filter by minimum transaction event count.''',
        require=False, validate=validators.Integer())

    minduration = Option(
        doc='''
        **Syntax:** **value=***<duration>*
        **Description:** Filter by minimum transaction duration.''',
        require=False, validate=validators.Integer())

    minstartdaysago = Option(
        doc='''
        **Syntax:** **value=***<integer>*
        **Description:** Filter by minimum transaction start time.''',
        require=False, validate=validators.Integer())

    minenddaysago = Option(
        doc='''
        **Syntax:** **value=***<integer>*
        **Description:** Filter by minimum transaction end time.''',
        require=False, validate=validators.Integer())
        
    transaction_id = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** Filter by field which is used as unique identifier for the transaction.''',
        require=False, validate=validators.Fieldname())

    tag_txn = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** Filter by field used as tag for the transaction.''',
        require=False)

    closed_txn = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** Filter by field used as status for the transaction.''',
        require=False)
        
    testmode = Option(
        doc='''
        **Syntax:** **value=***<boolean>*
        **Description:** Set **testmode** to true if the results should not be written to the index. Default is **false**.''',
        require=False, default=False, validate=validators.Boolean())

    splunk_server = Option(
        doc='''
        **Syntax:** **value=***<splunk_server>*
        **Description:** Set **splunk_server** to the Splunk Server' host name holding the index.''',
        require=False, default='localhost')
     
    splunkd_port = Option(
        doc='''
        **Syntax:** **value=***<splunkd_port>*
        **Description:** Set **splunkd_port** to the Splunk Server's splunkd port holding the index.''',
        require=False, default=8089)
 
    host = Option(
        doc='''
        **Syntax:** **value=***<host>*
        **Description:** Set **host** to the value you want the host field to hold.''',
        require=False)
        
    source = Option(
        doc='''
        **Syntax:** **value=***<source>*
        **Description:** Set **source** to the value you want the source field to hold.''',
        require=False)
      
    sourcetype = Option(
        doc='''
        **Syntax:** **value=***<sourcetype>*
        **Description:** Set **sourcetype** to the value you want the sourcetype field to hold.''',
        require=False)
        
    action = Option(
        doc='''
        **Syntax:** **value=***<copy|move|flush>*
        **Description:** Set **action** to copy, move or flush. Default's to move''',
        require=True, default='move')

    collection = Option(
        doc='''
        **Syntax:** **value=***<collection>*
        **Description:** Set **collection** to the KV store to read from.''',
        require=True)
        
    app = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** Set **app** to the app containing the KV store definition.''',
        require=False, default="SA-kvtransaction")

    index = Option(
        doc='''
        **Syntax:** **value=***<index>*
        **Description:** Set **index** to the index to write to.''',
        require=True)


        
    def generate(self):
        ## Initialize an app service to communicate via REST
        #
        sessionKey = self.input_header["sessionKey"]
        
        if not self.host:
            self.host       = self.splunk_server
        if not self.source:
            self.source     = self.collection
        if not self.sourcetype:
            self.sourcetype = self.collection


        """                                       """
        """   Retrieve and filter transactions.   """
        """                                       """
        
        ## Assemble filter query for requesting KV store entries
        #
        filter       = []
        current_time = datetime.now()
            
        if self.tag_txn:
            filter.append({'tag_txn': str(self.tag_txn)})
            
        if self.closed_txn:
            filter.append({'closed_txn': str(self.closed_txn)})
            
        if self.minevents:
            filter.append({'event_count': {'$gte': int(self.minevents)}})
            
        if self.minduration:
            filter.append({'duration': {'$gte': str(self.minduration)}})

        if self.minstartdaysago:
            delta      = timedelta(days=self.minstartdaysago)
            timefilter = current_time - delta
            filter.append({'_time': {'$lte': str((timefilter - datetime(1970,1,1)).total_seconds())}})

        """
        ## TODO: Currently not working. Yields 'query invalid', even when modifying the not statement.
        #
        if self.transaction_id:
            filter.append({str(self.transaction_id): {'$not': ''}})
          
             
        ## TODO: This parameter is not supported atm since the transaction's duration has to be known
        ## Current behaviour is equal to minstartdaysago
        #
        if self.minenddaysago:
            delta      = timedelta(days=self.minenddaysago)
            timefilter = current_time - delta
            filter.append({'_time': {'$lt': timefilter.strftime('%Y-%d-%d %H:%M:%S.%f')}})
        """
        
        query = {"$and": filter}


        ## Get kv store entries according to the set parameters
        #
        #self.logger.debug("Filter for transaction ids: %s." % query)
        if len(filter) > 0:
            uri = '/servicesNS/nobody/%s/storage/collections/data/%s?query=%s' % (self.app, self.collection, urllib.quote(json.dumps(query)))
        else:
            uri = '/servicesNS/nobody/%s/storage/collections/data/%s' % (self.app, self.collection)
        serverResponse, serverContent = rest.simpleRequest(uri, sessionKey=sessionKey)
        transactions                  = json.loads(serverContent)
        transactions_dict             = {item['_key']:collections.OrderedDict(item) for item in transactions}
            
        ## Print retrieved events
        #
        for transaction in transactions_dict:
            #self.logger.debug("Transaction: %s." % transaction)
            yield transactions_dict.get(transaction, {})

            
        """                                        """
        """   Copy / move / delete transactions.   """
        """                                        """
        
        ## Perform according to the set action if testmode is not true
        #
        if not self.testmode:
            if re.match(r'copy', str(self.action)):
                ## Open connection and copy data read from collection to index
                #
                token   = ("Splunk %s" % sessionKey)
                service = client.connect(host=self.splunk_server, port=self.splunkd_port, token=token)
                index   = service.indexes[self.index]
                #socket  = index.attach(host=self.host, source=self.source, sourcetype=self.sourcetype)

                for transaction in transactions_dict:
                    data = transactions_dict.get(transaction, {})
                    del data['_key']
                    del data['_user']
                    del data['_hashes']
                    try:
                        del data['tag_txn']
                    except:
                        pass
                    try:
                        del data['closed_txn']
                    except:
                        pass
                    for key in data:
                        if '__latest_' in key:
                            del data[key]

                    json_data = json.dumps(data, sort_keys=True)
                    #socket.send("%s\n\n" % json_data)
                    #socket.send("\n")
                    index.submit("%s\n\n" % json_data, host=self.host, source=self.source, sourcetype=self.sourcetype)
                #socket.close()

            elif re.match(r'move', str(self.action)):
                ## Open connection and copy data read from collection to index
                #
                token   = ("Splunk %s" % sessionKey)
                service = client.connect(host=self.splunk_server, port=self.splunkd_port, token=token)
                index   = service.indexes[self.index]

                for transaction in transactions_dict:
                    data = transactions_dict.get(transaction, {})
                    del data['_key']
                    del data['_user']
                    del data['_hashes']
                    try:
                        del data['tag_txn']
                    except:
                        pass
                    try:
                        del data['closed_txn']
                    except:
                        pass
                    for key in data:
                        if '__latest_' in key:
                            del data[key]
                            
                    json_data = json.dumps(data, sort_keys=True)
                    index.submit("%s\n\n" % json_data, host=self.host, source=self.source, sourcetype=self.sourcetype)

                ## Remove read data from collection
                ## TODO: Create a list of _key after filtering above and do this by key?
                ##       Only neccessary if filtering by transaction_id and/or minenddaysago will be implemented.
                #
                if len(filter) > 0:
                    uri = '/servicesNS/nobody/%s/storage/collections/data/%s?query=%s' % (self.app, self.collection, urllib.quote(json.dumps(query)))
                else:
                    uri = '/servicesNS/nobody/%s/storage/collections/data/%s' % (self.app, self.collection)
                rest.simpleRequest(uri, sessionKey=sessionKey, method='DELETE')

            elif re.match(r'flush', str(self.action)):
                ## Remove read data from collection
                #
                if len(filter) > 0:
                    uri = '/servicesNS/nobody/%s/storage/collections/data/%s?query=%s' % (self.app, self.collection, urllib.quote(json.dumps(query)))
                else:
                    uri = '/servicesNS/nobody/%s/storage/collections/data/%s' % (self.app, self.collection)
                rest.simpleRequest(uri, sessionKey=sessionKey, method='DELETE')

            else:
                raise ValueError('The argument "action" is invalid: %s. Set to copy, move or flush.' % self.action)

        elif self.testmode and re.match(r'(copy|move|flush)', str(self.action)) is None:
            raise ValueError('The argument "action" is invalid: %s. Set to copy, move or flush.' % self.action)

        else:
            pass

dispatch(outputkvtransaction, sys.argv, sys.stdin, sys.stdout, __name__)
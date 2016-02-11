#!/usr/bin/env python

import sys, json, collections
import splunklib.client as client

from splunklib.searchcommands import \
    dispatch, ReportingCommand, Configuration, Option, validators


def update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d
    
@Configuration(clear_required_fields=True, requires_preop=True)
class outputTransKVCommand(ReportingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    """
    
    kv_store = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** name of the collection to be used for saving transaction results''',
        require=True)

    primary_key = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** name of the collection to be used for saving transaction results''',
        require=True, validate=validators.Fieldname())

    testmode = Option(
        doc='''
        **Syntax:** **value=***<string>*
        **Description:** name of the collection to be used for saving transaction results''',
        require=False, default=False, validate=validators.Boolean())

    collections_data_endpoint = 'storage/collections/data/'
    output_array = []

    
    def reduce(self, events):
        # create empty result dictionary
        result_dict = dict()
        
        # loop through events and update the result dictionary
        for event in events:
            event_dict = {event[self.primary_key]: event}
            update(result_dict, event_dict)
        
        # output the values of the result dictionary
        for k, v in result_dict.iteritems():
            yield v


dispatch(outputTransKVCommand, sys.argv, sys.stdin, sys.stdout, __name__)

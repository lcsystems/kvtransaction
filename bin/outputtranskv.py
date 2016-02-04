#!/usr/bin/env python

import sys, json
import splunklib.client as client

from splunklib.searchcommands import \
    dispatch, ReportingCommand, Configuration, Option, validators


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
        # Put your event transformation code here
        for event in events:
            event["_key"] = event[self.primary_key]
            self.output_array.append(event)
            if (len(self.output_array)%1000 == 0) and not self.testmode:
                app_service = client.Service(token=self.input_header["sessionKey"])
                request2 = app_service.request(
                    self.collections_data_endpoint + self.kv_store + "/batch_save",
                    method = 'post',
                    headers = [('content-type', 'application/json')],
                    body = json.dumps(self.output_array),
                    owner = 'nobody',
                    app = 'SA-transactional_kv'
                )
                self.output_array = []
            yield {"type": type(event), "event": event, "json": "[" + json.dumps(event) + "]"}


dispatch(outputTransKVCommand, sys.argv, sys.stdin, sys.stdout, __name__)

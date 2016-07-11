# Splunk App SA-kvtransaction

## Copyright 2016 © LC Systems, Switzerland | Germany | Austria.
## This Splunk App may only be used in agreement with LC Systems.

## Author: christoph.dittmann@lcsystems.de, harun.kuessner@lcsystems.de, mika.borner@lcsystems.ch
## Usage:  As a Splunk Supporting Add-on on Search Heads


## Changelog

- v1.8.5b
        - Optimized performance
        
- v1.8.4b
        - Fixed undocumented bug causing the API requests to the kv store to never be chunked
        
        - Added option "app" to both commands to set the app holding the collection definition
        
- v1.8.3b
        - Fixed some undocumented bugs bugs in the kvtransaction command

- v1.8.2b
        - Fixed undocumented bugs in both commands
        
        - Performance optimizations for both commands
        
- v1.8.1b
        - Fixed a bug with mvlist writing empty fields to the kv store
        
        - Renamed options tag to tag_txn and status to closed_txn
        
- v1.8b
        - Now using Splunk SDK for Python version 1.6.0
        
        - Removed neccessity to provide the command with login credentials
        
        - Renamed "outputkvtransaction" to "kvtransactionoutput"

- v1.7.2a
        - Fixed a bug with transactions not being pulled from the collection under certain circumstances
        
        - Improved performance for retrieving transactions from the kv store

- v1.7.1a
        - Fixed a bug with the custom validator having no effect
        
        - Fixed a bug with too long URLs resulting in HTTP 414 errors when making REST requests to the kv store.

- v1.7a
        - Modified kvtransactionoutput's method to connect and send data to indexers

- v1.6a
        - Added parameters to kvtransactionoutput to make rerouting and setting metadata possible.
          The command is now fully functional despite for the optional parameters transaction_id and minenddaysago, which might be removed anyway.

- v1.5.2a
        - Slight code optimizations

- v1.5.1a
        - BUGFIX: Incomplete backfilling

- v1.5a
        - Now using Search Command Protocol v2 (chunked processing mode)
        
        - Further code optimization
        
        - Renamed field "hashes" to "_hashes" to prevent it from getting displayed

- v1.4.6
        - Code restructuration and optimization
        
- v1.4.5
        - Now using Splunk SDK for Python version 1.5.0
        
        - Bugfixing regarding event aggregation and attribute calculation, slight code modifications
        
        - Events which did not actively contribute to a transaction will not cause a stored transaction to be displayed

- v1.4  
        - Added initial version of the "kvtransactionoutput" command for collection housekeeping

        - Prettyfied README file

- v1.3  
        - Implemented checksums to enable proper collection backfilling

- v1.2  
        - Added README file

        - Added verbose comments in code
        
        - Added mvdedup parameter
        
        - Added custom validator to mvlist to accept boolean values or lists of fieldnames
        
        - Fixed event visualization for multivalued transactions
        
        - Corrected calculation of duration and event_count


## Known issues

- kvtransaction

        - URLs for requesting collections via REST are restricted in length limiting the amount of transactions which can be retrieved at once. 
          The currently implemented workaround (line 170) has significant impact on the command's performance.
          A well performing but unlogical solution is implemented from line 195.

        - Transactions won't be displayed in the correct time order

- kvtransactionoutput

        - Does not currently return events in correct timeorder

- Validators do not accept comma- or space-separated values. This is "working as designed". See: http://docs.splunk.com/Documentation/PythonSDK).
  Thus the custom validator for mvlist only accepts boolean values, single field values or comma- and/or space-separated lists in quotes.
  Workaround: Put comma-space-separated list in quotes. ... mvlist="value1, value2, value3" ...


## TODO

- Fix issues listed above

- Remove debug logging from release version when finished developing


## TBD

- kvtransaction

        - TBD: Add parameters maxspan, maxpause, maxevents, force_update

        - TBD: Add handling for optional fields status, tag, end_time
        
        - TBD: mvdedup also deduplicates already stored entries

- kvtransactionoutput

        - TBD: Not all planned parameters are implemented/working at the moment (see comments in code)

        - TBD: Add checksumming too to prevent events from contributing multiple times
        
        
## Installation

- Installation on Search Heads as usual (ensure the app folder is named "SA-kvtransaction")

- Create collections as needed containing the mandatory fields _time, duration, event_count and your custom fields

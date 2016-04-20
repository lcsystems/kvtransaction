# Splunk App SA-kvtransaction

## Copyright 2016 © LC Systems, Switzerland | Germany | Austria.
## This Splunk App may only be used in agreement with LC Systems.

## Author: christoph.dittmann@lcsystems.de, harun.kuessner@lcsystems.de
## Usage:  As a Splunk Supporting Add-on on Search Heads


## Changelog

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
        - Added initial version of the "outputkvtransaction" command for collection housekeeping

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

        - With the current implementation of Search Command Protocol v2 it is not possible to retrieve the current session key,
          thus valid login credentials have to be provided in plain text inside the script.
        
        - Transactions won't be displayed in the correct time order

        - When setting mvlist=f only the latest event is written to the kv store (expected?)

        - mvdedup deduplicates all values, even the ones already stored (expected?)

- outputkvtransaction

        - Does not currently return events in correct timeorder
  
        - Events retrieved from KV Store currently cannot be displayed as raw events (use table * as a workaround)

- Custom validator does not accept comma- or space-separated values (this seems to be "working as designed")


## TODO

- Bugfixing

- Remove debug logging from release version when finished developing

- kvtransaction

        - TBD: Add parameters maxspan, maxpause and maxevents

        - TBD: Add handling for optional fields status, tag, end_time

- outputkvtransaction

        - Not all parameters are implemented/working at the moment (see comments in code)


## Installation

- Installation on Search Heads as usual

- Create collections as needed containing the mandatory fields _time, duration, event_count, _hashes

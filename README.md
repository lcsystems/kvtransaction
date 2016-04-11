## Splunk App SA-kvtransaction
#
# Copyright 2016 © LC Systems, Switzerland | Germany | Austria.
# This Splunk App may only be used in agreement with LC Systems.
#
# Author: christoph.dittmann@lcsystems.de, harun.kuessner@lcsystems.de
# Usage:  As a Splunk Supporting Add-on on Search Heads


## Changelog
#
- v1.2  - Added README file
        - Added verbose comments in code
        - Added mvdedup parameter
        - Added custom validator to mvlist to accept boolean values or lists of fieldnames
        - Fixed event visualization for multivalued transactions
        - Corrected calculation of duration and event_count

## Known issues
#
- When event_count hits 2500 a new event begins
- When setting mvlist=f only the latest event is written to the kv store (expected?)
- mvdedup deduplicates all values, even the ones already stored (expected?)
- Custom validator does not accept comma- or space-separated values (this seems to be "working as designed")

## TODO
#
- Optimize code as it is mainly functional at the moment
- Implement checksums to be able to do a proper backfill without duplication
- Implement commands for housekeeping (move closed transactions to a summary)
        
## Installation
#
- Installation on Search Heads as usual
- Create collections as needed containing the mandatory fields _time, duration, event_count
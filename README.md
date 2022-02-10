# snapshot-merger
Tool to merge snapshot tables to reveal data history.

A snapshot table is a particular type of table in which data is stored with a "date" field which determines its state in that exact moment.
This kind of tables is really common in Big Data environments and is useful when you are interested in looking at the state of your data on a certain day, but not if want to know how your data has changed during time.
With this tool you are able to merge the snapshots comparing them with a combination of key-fields and analyze for each entry when it has been created, edited or deleted.
The resulting table is then stored as an HBase table to optimize data retrieval.

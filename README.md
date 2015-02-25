# SqlServer - ElasticSearch Sync

ElasticSeatchSync is a tool that enables synchronization between Sql Server databases and elasticsearch indices.

It uses ADO.Net for sql connection and query executions, and Elasticsearch.Net library for elasticsearch connection and bulk actions.

ElasticSearchSync allows to generate bulks that performs index and delete actions, from a sql server source.

You may need to see [Environment setup](EnvironmentSetup.md) first.

### NuGet intallation

`PM> Install-Package ElasticSearchSync`

### Differences with JDBC plugin

This library was implemented after discovering some important bugs appeared in JDBC plugin river that didn't allow it to be configured as an incremental process (only fetch data from sql server that has been created or updated after the last river run).

This issues were covered with a state persistence log.

Of course this tool *doesn't* have the total of JDBC features.

### Features

Some of the features that this library try implements are:

- Can be setup to only sync data that has been created or modified since the last run.
- Create logs of all bulk performances, and a summary of the whole sync execution (sync state log).
- Better performance of serialized json objects that contain arrays.
- As it uses Elasticsearch.Net, it enables to configure a `connection pool` with the totality of nodes that live in the cluster, so it is a failover client, it isn't atached to only one node.


## ToDo
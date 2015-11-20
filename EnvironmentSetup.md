# Environment setup

This library is tested using the following tools and versions of them:

- Java SE 1.8.0.31
- Elasticsearch 1.4.2
- Sql Server 2012  

### Elasticsearch setup 
##### This guide is oriented for Windows envirenments
1. [Download and install Java](http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html)
2. Set JAVA\_HOME path.
3. [Download and extract elasticsearch](http://www.elasticsearch.org/download) ([recommended guide](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/_installation.html)).
4. Install elasticsearch plugins. Some usefull plugins for monitoring and query debug:
	1. [Marvel](http://www.elasticsearch.org/overview/marvel/)
	2. [Head](https://github.com/mobz/elasticsearch-head)
	3. [Inquisitor](https://github.com/polyfractal/elasticsearch-inquisitor)
	4. [HQ](http://www.elastichq.org/)
	
	*NOTE:* easiest way to install elasticsearch plugins is using [curl](http://www.paehl.com/open_source/?CURL_7.40.0).

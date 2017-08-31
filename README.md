Pentaho Cassandra Plugin
========================

This Pentaho Cassandra Plugin Project provides support for the Cassandra 3.x with DataStax driver
It is a plugin for the Pentaho Kettle engine which can be used within Pentaho Data Integration (Kettle), Pentaho Reporting, and the Pentaho BI Platform.

This plugin is based on https://github.com/AutSoft/pentaho-cassandra-connector
I added :
- Management of null values in output steps (Not inserting null with dynamic INSERT statements)
- Management of all CQL datatypes

Building
--------

    $ git clone git://github.com/bcolas/pentaho-cassandra-plugin.git
    $ cd pentaho-cassandra-plugin
    $ ant

This will produce a plugin archive in dist/pentaho-cassandra-plugin-${project.revision}.tar.gz (and .zip). This archive can then be extracted into your Pentaho Data Integration plugin directory.

Test
----

Tested on Pentaho Data Integration 6.1

License
-------
Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.

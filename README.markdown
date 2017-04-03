# Akka Persistence Redis Plugin

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [What is it?](#what-is-it)
- [(De)Serializion](#deserializion)
- [Using the Journal Component](#using-the-journal-component)
  - [Tags](#tags)
- [Using the Journal Query Interface Component](#using-the-journal-query-interface-component)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## What is it?

Akka Persistence Redis Plugin is a plugin for Akka persistence that provides several components:
 - a journal store ;
 - a snapshot store ;
 - a journal query interface implementation.

This plugin stores data in a [redis](https://redis.io) database.

## (De)Serializion

The journal component saves serialized values into the database.
It relies on the [Akka serialization extension](http://doc.akka.io/docs/akka/2.4/scala/serialization.html).
Custom serialization can be added to handle you data model elements as you wish.

## Using the Journal Component

To use the journal component, you need to enable it in your configuration. To use the default settings, simply add this line:

```scala
akka.persistence.journal.plugin = "akka-persistence-redis.journal"
```

### Tags

The journal component has support for tagged events.
All events wrapped inside an instance of class `akka.persistence.journal.Tagged` are associated to the provided tags in the data store.

## Using the Journal Query Interface Component

To use the journal query component, you need to enable it in your configuration. To use the default settings, simply add this line:

```scala
import akka.persistence.query._
import akka.persistence.query.journal.redis._

val readJournal = PersistenceQuery(system)
  .readJournalFor[ScalaReadJournal]("akka-persistence-redis.read-journal")
```

For more details on the available capabilities of the journal query, please refer to the API documentation.

## License

This work is inspired by [Akka Persistence Redis Plugin](https://github.com/hootsuite/akka-persistence-redis) by HootSuite Media Inc licensed under Apache license version 2.

Copyright © 2017 Safety Data - CFH SAS.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

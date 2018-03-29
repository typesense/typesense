<a href="https://typesense.org"><img src="assets/typesense_medium.png?raw=true" alt="Typesense" width="298" /></a>

Typesense is a fast, typo-tolerant search engine for building delightful search experiences.

<img src="assets/typesense_books_demo.gif?raw=true" alt="Typesense Demo" width="459" />

## Menu

- [Features](#features)
- [Install](#install)
- [Quick Start](#quick-start)
- [Detailed Guide](#detailed-guide)
- [Build from Source](#build-from-source)
- [Help](#help)

## Features

- **Typo tolerant:** Handles typographical errors elegantly.
- **Simple and delightful:** Simple to set-up and manage.
- **Tunable ranking:** Easy to tailor your search results to perfection.
- **Fast:** Meticulously designed and optimized for speed.

## Install

You can download the [binary packages](https://github.com/wreally/typesense/releases) that we publish for 
Linux (x86-64) and Mac.

You can also run Typesense from our [official Docker image](https://hub.docker.com/r/typesense/typesense/):

```
docker run -p 8108:8108 -v/tmp/data:/data typesense/typesense:0.8.0 --data-dir /data --api-key=Hu52dwsas2AdxdE
```

## Quick Start

Here's a quick example showcasing how you can create a collection, index a document and search it on Typesense. 

Let's initialize the client and create a `companies` collection:

```python
import typesense

client = typesense.Client({
  'master_node': {
    'host': 'localhost',
    'port': '8108',
    'protocol': 'http',
    'api_key': 'abcd'
  },
  'timeout_seconds': 2
})

create_response = client.collections.create({
  "name": "companies",
  "fields": [
    {"name": "company_name", "type": "string" },
    {"name": "num_employees", "type": "int32" },
    {"name": "country", "type": "string", "facet": true }
  ],
  "default_sorting_field": "num_employees"
})
```

Now, let's add a document to the collection we just created:

```python
document = {
 "id": "124",
 "company_name": "Stark Industries",
 "num_employees": 5215,
 "country": "USA"
}

client.collections['companies'].documents.create(document)
```

Finally, let's search for the document we just indexed:

```python
search_parameters = {
  'q'         : 'stork',
  'query_by'  : 'company_name',
  'filter_by' : 'num_employees:>100',
  'sort_by'   : 'num_employees:desc'
}

client.collections['companies'].documents.search(search_parameters)
```

**Did you notice the typo in the query text?** No big deal. Typesense handles typographic errors out-of-the-box!

## Detailed Guide

A detailed guide is available on [Typesense website](https://typesense.org/docs). 

## Build from source

**Building with Docker**

The docker build script takes care of all required dependencies, so it's the easiest way to build Typesense:

```
$ TYPESENSE_IMG_VERSION=nightly ./docker-build.sh --build-deploy-image [--clean] [--depclean]
```

**Building on your machine**

Typesense requires the following dependencies: 

* C++11 compatible compiler (GCC >= 4.9.0, Apple Clang >= 8.0, Clang >= 3.9.0)
* Snappy
* zlib
* OpenSSL (>=1.0.2)
* curl

```
$ ./build.sh [--clean] [--depclean]
```

The first build will take some time since other third-party libraries are pulled and built as part of the build process.

## Help

If you've any questions or run into any problems, please create a Github issue and we'll try our best to help.

&copy; 2016-2018 Wreally Studios Inc.
<p align="center">
  <a href="https://typesense.org"><img src="assets/typesense_logo.svg" alt="Typesense" width="298" /></a> 
</p>
<p align="center">
  Typesense is a fast, typo-tolerant search engine for building delightful search experiences.
</p>

<p align="center">
  An Open Source Algolia Alternative & <br>
  An Easier-to-Use ElasticSearch Alternative
</p>

<p align="center">
 <!-- <a href="https://circleci.com/gh/typesense/typesense"><img src="https://circleci.com/gh/typesense/typesense.svg?style=shield&circle-token=1addd775339738a3d90869ddd8201110d561feaa"></a> -->
 <a href="https://hub.docker.com/r/typesense/typesense/tags"><img src="https://img.shields.io/docker/pulls/typesense/typesense"></a>
  <a href="https://github.com/typesense"><img src="https://img.shields.io/github/stars/typesense/typesense?label=github%20stars"></a>
<p>
<p align="center">
  <a href="https://typesense.org">Website</a> | 
  <a href="https://typesense.org/docs/">Documentation</a> | 
  <a href="https://github.com/orgs/typesense/projects/1">Roadmap</a> | 
  <a href="https://join.slack.com/t/typesense-community/shared_invite/zt-mx4nbsbn-AuOL89O7iBtvkz136egSJg">Slack Community</a> | 
  <a href="https://threads.typesense.org/kb">Community Threads</a> | 
  <a href="https://twitter.com/typesense">Twitter</a> | 
  <a href="https://calendly.com/jason-typesense/typesense-office-hours">Office Hours</a>
</p>
<br>
<p align="center">
  <img src="assets/typesense_books_demo.gif?raw=true" alt="Typesense Demo" width="459" />
</p>

✨ Here are a couple of **live demos** that show Typesense in action on large datasets:

- Search a 32M songs dataset from MusicBrainz: [songs-search.typesense.org](https://songs-search.typesense.org/)
- Search a 28M books dataset from OpenLibrary: [books-search.typesense.org](https://books-search.typesense.org/)
- Search a 2M recipe dataset from RecipeNLG: [recipe-search.typesense.org](https://recipe-search.typesense.org/)
- Search 1M Git commit messages from the Linux Kernel: [linux-commits-search.typesense.org](https://linux-commits-search.typesense.org/)
- Spellchecker with type-ahead, with 333K English words: [spellcheck.typesense.org](https://spellcheck.typesense.org/)
- An E-Commerce Store Browsing experience: [ecommerce-store.typesense.org](https://ecommerce-store.typesense.org/)
- GeoSearch / Browsing experience: [airbnb-geosearch.typesense.org](https://airbnb-geosearch.typesense.org/)
- Search / Browse xkcd comics by topic: [xkcd-search.typesense.org](https://xkcd-search.typesense.org/)

🗣️ 🎥 If you prefer watching videos:

- Here's one where we introduce Typesense and show a walk-through: https://youtu.be/F4mB0x_B1AE?t=144
- Here's our [roadmap](https://github.com/orgs/typesense/projects/1) call from Q1 2022: https://aviyel.com/events/297/typesense-community-call-q1-2022-roadmap-and-contributor-spotlight
- Check out Typesense's recent mention during Google I/O Developer Keynote: https://youtu.be/qBkyU1TJKDg?t=2399

## Quick Links

- [Features](#features)
- [Benchmarks](#benchmarks)
- [Roadmap](#roadmap)
- [Who's using this](#whos-using-this)
- [Install](#install)
- [Quick Start](#quick-start)
- [Step-by-step Walk-through](#step-by-step-walk-through)
- [API Documentation](#api-documentation)
- [API Clients](#api-clients)
- [Search UI Components](#search-ui-components)
- [FAQ](#faq)
- [Support](#support)
- [Contributing](#contributing)
- [Getting Latest Updates](#getting-latest-updates)
- [Build from Source](#build-from-source)

## Features

- **Typo Tolerance:** Handles typographical errors elegantly, out-of-the-box.
- **Simple and Delightful:** Simple to set-up, integrate with, operate and scale.
- **⚡ Blazing Fast:** Built in C++. Meticulously architected from the ground-up for low-latency (<50ms) instant searches.
- **Tunable Ranking:** Easy to tailor your search results to perfection.
- **Sorting:** Sort results based on a particular field at query time (helpful for features like "Sort by Price (asc)").
- **Faceting & Filtering:** Drill down and refine results.
- **Grouping & Distinct:** Group similar results together to show more variety.
- **Federated Search:** Search across multiple collections (indices) in a single HTTP request.
- **Geo Search:** Search and sort by results around a geographic location.
- **Vector search:** support for both exact & HNSW-based approximate vector searching.
- **Scoped API Keys:** Generate API keys that only allow access to certain records, for multi-tenant applications.
- **Synonyms:** Define words as equivalents of each other, so searching for a word will also return results for the synonyms defined.
- **Curation & Merchandizing:** Boost particular records to a fixed position in the search results, to feature them.
- **Raft-based Clustering:** Setup a distributed cluster that is highly available.
- **Seamless Version Upgrades:** As new versions of Typesense come out, upgrading is as simple as swapping out the binary and restarting Typesense.
- **No Runtime Dependencies:** Typesense is a single binary that you can run locally or in production with a single command.

**Don't see a feature on this list?** Search our issue tracker if someone has already requested it and add a comment to it explaining your use-case, or open a new issue if not. We prioritize our roadmap based on user feedback, so we'd love to hear from you. 

## Roadmap

Here's Typesense's public roadmap: [https://github.com/orgs/typesense/projects/1](https://github.com/orgs/typesense/projects/1).

The first column also explains how we prioritize features, how you can influence prioritization and our release cadence. 

## Benchmarks

- A dataset containing **2.2 Million recipes** (recipe names and ingredients):
  - Took up about 900MB of RAM when indexed in Typesense
  - Took 3.6mins to index all 2.2M records
  - On a server with 4vCPUs, Typesense was able to handle a concurrency of **104 concurrent search queries per second**, with an average search processing time of **11ms**.
- A dataset containing **28 Million books** (book titles, authors and categories):
  - Took up about 14GB of RAM when indexed in Typesense
  - Took 78mins to index all 28M records
  - On a server with 4vCPUs, Typesense was able to handle a concurrency of **46 concurrent search queries per second**, with an average search processing time of **28ms**.
- With a dataset containing **3 Million products** (Amazon product data), Typesense was able to handle a throughput of **250 concurrent search queries per second** on an 8-vCPU 3-node Highly Available Typesense cluster.

We'd love to benchmark with larger datasets, if we can find large ones in the public domain. If you have any suggestions for structured datasets that are open, please let us know by opening an issue. We'd also be delighted if you're able to share benchmarks from your own large datasets. Please send us a PR! 

## Who's using this?

Typesense is used by a range of users across different industries. We've only recently started documenting who's using it in our [Showcase](SHOWCASE.md).

If you'd like to be included in the list, please feel free to edit [SHOWCASE.md](SHOWCASE.md) and send us a PR.

You'll also see a list of user logos on the [Typesense Cloud](https://cloud.typesense.org) home page.

## Install

**Option 1:** You can download the [binary packages](https://typesense.org/downloads) that we publish for 
Linux (x86-64) and Mac.

**Option 2:** You can also run Typesense from our [official Docker image](https://hub.docker.com/r/typesense/typesense).

**Option 3:** Spin up a managed cluster with [Typesense Cloud](https://cloud.typesense.org):

<a href="https://cloud.typesense.org"><img src="assets/deploy_with_typesense_cloud.svg?raw=true" alt="Deploy with Typesense Cloud" height="60" /></a> 

## Quick Start

Here's a quick example showcasing how you can create a collection, index a document and search it on Typesense.
 
Let's begin by starting the Typesense server via Docker:

```
docker run -p 8108:8108 -v/tmp/data:/data typesense/typesense:0.24.1 --data-dir /data --api-key=Hu52dwsas2AdxdE
```

We have [API Clients](#api-clients) in a couple of languages, but let's use the Python client for this example.

Install the Python client for Typesense:
 
```
pip install typesense
```

We can now initialize the client and create a `companies` collection:

```python
import typesense

client = typesense.Client({
  'api_key': 'Hu52dwsas2AdxdE',
  'nodes': [{
    'host': 'localhost',
    'port': '8108',
    'protocol': 'http'
  }],
  'connection_timeout_seconds': 2
})

create_response = client.collections.create({
  "name": "companies",
  "fields": [
    {"name": "company_name", "type": "string" },
    {"name": "num_employees", "type": "int32" },
    {"name": "country", "type": "string", "facet": True }
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

## Step-by-step Walk-through

A step-by-step walk-through is available on our website [here](https://typesense.org/guide). 

This will guide you through the process of starting up a Typesense server, indexing data in it and querying the data set. 

## API Documentation

Here's our official API documentation, available on our website: [https://typesense.org/api](https://typesense.org/api).

If you notice any issues with the documentation or walk-through, please let us know or send us a PR here: [https://github.com/typesense/typesense-website](https://github.com/typesense/typesense-website).

## API Clients

While you can definitely use CURL to interact with Typesense Server directly, we offer official API clients to simplify using Typesense from your language of choice. The API Clients come built-in with a smart retry strategy to ensure that API calls made via them are resilient, especially in an HA setup.

- [typesense-js](https://github.com/typesense/typesense-js)
- [typesense-php](https://github.com/typesense/typesense-php)
- [typesense-python](https://github.com/typesense/typesense-python)
- [typesense-ruby](https://github.com/typesense/typesense-ruby)

If we don't offer an API client in your language, you can still use any popular HTTP client library to access Typesense's APIs directly. 

Here are some community-contributed clients and integrations:

- [API client for Go](https://github.com/typesense/typesense-go)
- [API client for Dart](https://github.com/typesense/typesense-dart)
- [API client for C#](https://github.com/DAXGRID/typesense-dotnet)
- [Laravel Scout driver](https://github.com/devloopsnet/laravel-scout-typesense-engine)
- [Symfony integration](https://github.com/acseo/TypesenseBundle)

We welcome community contributions to add more official client libraries and integrations. Please reach out to us at contact@typsense.org or open an issue on Github to collaborate with us on the architecture. 🙏

## Search UI Components

You can use our [InstantSearch.js adapter](https://github.com/typesense/typesense-instantsearch-adapter) 
to quickly build powerful search experiences, complete with filtering, sorting, pagination and more.

Here's how: [https://typesense.org/docs/0.24.1/guide/#search-ui](https://typesense.org/docs/0.24.1/guide/#search-ui) 

## FAQ

### How does this differ from Elasticsearch?

Elasticsearch is a large piece of software, that takes non-trivial amount of effort to setup, administer, scale and fine-tune. 
It offers you a few thousand configuration parameters to get to your ideal configuration. So it's better suited for large teams 
who have the bandwidth to get it production-ready, regularly monitor it and scale it, especially when they have a need to store 
billions of documents and petabytes of data (eg: logs).

Typesense is built specifically for decreasing the "time to market" for a delightful search experience. It's a light-weight
yet powerful & scaleable alternative that focuses on Developer Happiness and Experience with a clean well-documented API, clear semantics 
and smart defaults so it just works well out-of-the-box, without you having to turn many knobs.

Elasticsearch also runs on the JVM, which by itself can be quite an effort to tune to run optimally. Typesense, on the other hand, 
is a single light-weight self-contained native binary, so it's simple to setup and operate.

See a side-by-side feature comparison [here](https://typesense.org/typesense-vs-algolia-vs-elasticsearch-vs-meilisearch/).

### How does this differ from Algolia?

Algolia is a proprietary, hosted, search-as-a-service product that works well, when cost is not an issue. From our experience,
fast growing sites and apps quickly run into search & indexing limits, accompanied by expensive plan upgrades as they scale.

Typesense on the other hand is an open-source product that you can run on your own infrastructure or
use our managed SaaS offering - [Typesense Cloud](https://cloud.typesense.org). 
The open source version is free to use (besides of course your own infra costs). 
With Typesense Cloud we don't charge by records or search operations. Instead, you get a dedicated cluster
and you can throw as much data and traffic at it as it can handle. You only pay a fixed hourly cost & bandwidth charges 
for it, depending on the configuration your choose, similar to most modern cloud platforms. 

From a product perspective, Typesense is closer in spirit to Algolia than Elasticsearch. 
However, we've addressed some important limitations with Algolia: 

Algolia requires separate indices for each sort order, which counts towards your plan limits. Most of the index settings like 
fields to search, fields to facet, fields to group by, ranking settings, etc 
are defined upfront when the index is created vs being able to set them on the fly at query time.

With Typesense, these settings can be configured at search time via query parameters which makes it very flexible
and unlocks new use cases. Typesense is also able to give you sorted results with a single index, vs having to create multiple.
This helps reduce memory consumption.

Algolia offers the following features that Typesense does not have currently: personalization & server-based search analytics. For analytics, you can still instrument your search on the client-side and send search metrics to your web analytics tool of choice. 

We intend to bridge this gap in Typesense, but in the meantime, please let us know
if any of these are a show stopper for your use case by creating a feature request in our issue tracker. 

See a side-by-side feature comparison [here](https://typesense.org/typesense-vs-algolia-vs-elasticsearch-vs-meilisearch/).

### Speed is great, but what about the memory footprint?

A fresh Typesense server will consume about 30 MB of memory. As you start indexing documents, the memory use will 
increase correspondingly. How much it increases depends on the number and type of fields you index. 

We've strived to keep the in-memory data structures lean. To give you a rough idea: when 1 million 
Hacker News titles are indexed along with their points, Typesense consumes 165 MB of memory. The same size of that data 
on disk in JSON format is 88 MB. If you have any numbers from your own datasets that we can add to this section, please send us a PR!

### Why the GPL license?

From our experience companies are generally concerned when **libraries** they use are GPL licensed, since library code is directly integrated into their code and will lead to derivative work and trigger GPL compliance. However, Typesense Server is **server software** and we expect users to typically run it as a separate daemon, and not integrate it with their own code. GPL covers and allows for this use case generously **(eg: Linux is GPL licensed)**. Now, AGPL is what makes server software accessed over a network result in derivative work and not GPL. And for that reason we’ve opted to not use AGPL for Typesense. 

Now, if someone makes modifications to Typesense server, GPL actually allows you to still keep the modifications to yourself as long as you don't distribute the modified code. So a company can for example modify Typesense server and run the modified code internally and still not have to open source their modifications, as long as they make the modified code available to everyone who has access to the modified software.

Now, if someone makes modifications to Typesense server and distributes the modifications, that's where GPL kicks in. Given that we’ve published our work to the community, we'd like for others' modifications to also be made open to the community in the spirit of open source. **We use GPL for this purpose.** Other licenses would allow our open source work to be modified, made closed source and distributed, which we want to avoid with Typesense for the project’s long term sustainability.

Here's more background on why GPL, as described by Discourse: https://meta.discourse.org/t/why-gnu-license/2531. Many of the points mentioned there resonate with us.

Now, all of the above only apply to Typesense Server. Our client libraries are indeed meant to be integrated into our users’ code and so they use Apache license.

So in summary, AGPL is what is usually problematic for server software and we’ve opted not to use it. We believe GPL for Typesense Server captures the essence of what we want for this open source project. GPL has a long history of successfully being used by popular open source projects. Our libraries are still Apache licensed.

If you have specifics that prevent you from using Typesense due to a licensing issue, we're happy to explore this topic further with you. Please reach out to us.

## Support

👋 🌐 New: If you have general questions about Typesense, want to say hello or just follow along, we'd like to invite you to join our [Slack Community](https://join.slack.com/t/typesense-community/shared_invite/zt-mx4nbsbn-AuOL89O7iBtvkz136egSJg). 

We also do virtual office hours every Friday. Reserve a time slot [here](https://calendly.com/jason-typesense/typesense-office-hours).

If you run into any problems or issues, please create a Github issue and we'll try our best to help.

We strive to provide good support through our issue trackers on Github. However, if you'd like to receive private & prioritized support with:

- Guaranteed SLAs
- Phone / video calls to discuss your specific use case and get recommendations on best practices
- Private discussions over Slack
- Guidance around deployment, ops and scaling best practices
- Prioritized feature requests

We do offer Paid Support options. Please reach out to us at contact@typesense.org to sign up.

## Contributing

We are a lean team on a mission to democratize search and we'll take all the help we can get! If you'd like to get involved, here's information on where we could use your help: [Contributing.md](https://github.com/typesense/typesense/blob/master/CONTRIBUTING.md)

## Getting Latest Updates

If you'd like to get updates when we release new versions, click on the "Watch" button on the top and select "Releases only". Github will then send you notifications along with a changelog with each new release.

We also post updates to our Twitter account about releases and additional topics related to Typesense. Follow us here: [@typesense](https://twitter.com/typesense).

👋 🌐 New: We'll also post updates on our [Slack Community](https://join.slack.com/t/typesense-community/shared_invite/zt-mx4nbsbn-AuOL89O7iBtvkz136egSJg). 

## Build from source

**Building with Docker**

The docker build script takes care of all required dependencies, so it's the easiest way to build Typesense:

```
TYPESENSE_VERSION=nightly ./docker-build.sh --build-deploy-image --create-binary [--clean] [--depclean]
```

**Building on your machine**

Typesense requires the following dependencies: 

* C++11 compatible compiler (GCC >= 4.9.0, Apple Clang >= 8.0, Clang >= 3.9.0)
* Snappy
* zlib
* OpenSSL (>=1.0.2)
* curl
* ICU
* brpc
* braft

```
./build.sh --create-binary [--clean] [--depclean]
```

The first build will take some time since other third-party libraries are pulled and built as part of the build process.

---
&copy; 2016-present Typesense Inc.

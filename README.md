# Typesense

Typesense is an open source search engine for building a delightful search experience.

- **Typo tolerance:** Handles typographical errors out-of-the-box
- **Tunable ranking + relevancy:** Tailor your search results to perfection
- **Blazing fast:** Meticulously designed and optimized for speed
- **Simple and delightful:** Simple API, delightful out-of-the-box experience

## Dependencies

* [RocksDB](https://github.com/facebook/rocksdb)
* [libfor](https://github.com/cruppstahl/libfor/)
* [h2o](https://github.com/h2o/h2o)

## Building Typesense from source

First, clone the git repository with `--recursive` option.

### OS X

Install the following Homebrew dependencies:

```
brew install cmake
brew install openssl
brew install h2o
brew install rocksdb
```

Building libfor:

```
make -C external/libfor 
```

Finally, build Typesense:

```
./build.sh
```

&copy; 2016-2017 Wreally Studios Inc.
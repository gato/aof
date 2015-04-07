[![Build Status](https://travis-ci.org/gato/aof.svg?branch=master)](https://travis-ci.org/gato/aof)
[![Coverage Status](https://coveralls.io/repos/gato/aof/badge.svg?branch=master)](https://coveralls.io/r/gato/aof?branch=master)
#aoflib

aoflib is a library written in Go for parsing and rewriting redis aof file (redis incremental backup)

used by aofgrep (available soon)

## Todo
- [ ] test write
- [ ] change readline to use length instead of searching for \n
- [ ] change replace in readline for TrimSuffix

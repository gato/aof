#aoflib
[![Build Status](https://travis-ci.org/gato/aof.svg?branch=master)](https://travis-ci.org/gato/aof)
[![Coverage Status](https://coveralls.io/repos/gato/aof/badge.svg?branch=master)](https://coveralls.io/r/gato/aof?branch=master)

aoflib is a library written in Go for parsing and rewriting redis aof file (redis incremental backup)
used by [aofgrep](http://github.com/gato/aofgrep) (available soon)

## Todo
- [*] test writeString Ok
- [ ] test writeString with errors
- [ ] test ToAof OK
- [ ] test ToAof with errors
- [ ] change readline to use length instead of searching for \n
- [ ] change replace in readline for TrimSuffix

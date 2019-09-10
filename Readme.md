# aoflib

[![Build Status](https://travis-ci.org/gato/aof.svg?branch=master)](https://travis-ci.org/gato/aof)
[![Coverage Status](https://coveralls.io/repos/gato/aof/badge.svg?branch=master)](https://coveralls.io/r/gato/aof?branch=master)
[![GoDoc](https://godoc.org/github.com/gato/aof?status.svg)](http://godoc.org/github.com/gato/aof)
[![Go Report Card](http://goreportcard.com/badge/gato/aof)](http://goreportcard.com/report/gato/aof)

aoflib is a library written in Go for parsing and rewriting redis aof file (redis incremental backup)
used by [aofgrep](http://github.com/gato/aofgrep) (available soon)

## Todo

- [x] test writeString Ok
- [x] test writeString with errors
- [x] test ToAof OK
- [x] change readline to use length instead of searching for \n
- [x] change replace in readline for TrimSuffix
- [x] 100% test coverage for ReadOperation
- [x] 100% test coverage for Operation.ToAof
- [ ] extract similar code from ReadOperation and readLine
- [ ] fix travis build
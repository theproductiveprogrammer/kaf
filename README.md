# Kaf

A Simple, High Performance, Event Store (cf. [Apache Kafka](https://kafka.apache.org))

## Why?

When Kakfa is overkill for a simple Event Store.

## How?

Single [golang](https://golang.org) file. Run using:

```sh
$> go run kaf.go <addr> <path to data folder>
```

*Example:* `go run kaf.go 127.0.0.1:7749 ../kaf-data`

Save data using:

```
/put/logfile

POSTED Message Data
```

*Example:* `curl localhost:7749 /put/testlog -d @notes`

Get messages using:

```
/get/logfile?from=<msg number>
```

Example: `curl localhost:7749 /get/testlog?from=1`

Responds with:

```
KAF_MSGS | v1 | Num Messages
KAF_MSG | Msg Num | Size (\n)
Message Data
...
KAF_MSG | Msg Num | Size (\n)
Message Data
```

## Architecture

High performance Golang server - one [goroutine](https://tour.golang.org/concurrency/1) per message log. Uses [synchronous channel](https://tour.golang.org/concurrency/2) for communication. Writes to disk, reads from disk. Uses OS file caching.

Disk format:

```
KAF_DB | v1 | Start Msg Num (\n)
KAF_MSG | Msg Num | Size (\n)
Message Data
...
```

Disk format is easy to [`cat`](https://en.wikipedia.org/wiki/Cat_(Unix)) / [edit](https://www.vim.org) and examine.

## Transparency

**Kaf** tracks useful information about itself in the `_kaf` log file.

Each message is a JSON record with the following format:

```
{
  beg: <ISO-Format>,
  end: <ISO-Format>,
  statno: <live stat call number>,
  logs: [
  {
    name: <logfile name>,
    last: <msg no>,
    gets: <num>, puts: <num>
  },
  {
    name: <logfile name>,
    last: <msg no>,
    gets: <num>, puts: <num>,
    errs: <num>
  },
  ...
  ]
}
```

These can be accessed as usual with: `/get/_kaf?from=…` 


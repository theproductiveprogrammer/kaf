# Kaf

A Simple Event Store (cf. [Apache Kafka](https://kafka.apache.org))

## Why?

When Kakfa is overkill for a simple Event Store.

## How?

Single [golang](https://golang.org) file. Run using:

```sh
$> go run kaf.go <port> <data folder>
```

Save data using:

```h
/put/logfile

POSTED Message Data
```

Get messages using:

```
/get/logfile?from=<msg number>
```

Responds with:

```
KAF | Msg Num | Size (\n)
Message Data
...
KAF | Msg Num | Size (\n)
Message Data
```

## Architecture

High performance Golang server - one [goroutine](https://tour.golang.org/concurrency/1) per message log. Uses [synchronous channel](https://tour.golang.org/concurrency/2) for communication. Writes to disk, reads from disk. Uses OS file caching.

Disk format:

```
KAF | v1 (\n)
KAF | Msg Num | Size (\n)
Message Data
...
```

Disk format is easy to [`cat`](https://en.wikipedia.org/wiki/Cat_(Unix)) / [edit](https://www.vim.org) and examine.

## Transparency

**Kaf** tracks useful information about itself in the `_kaf` log file.

Each message is a JSON record with the following format:

```
{
  startTime: <ISO-Format>,
  endTime: <ISO-Format>,
  gets: [
    { logfile: ..., num: ... },
    { logfile: ..., num: ... },
    ...
  ],
  puts: [
    { logfile: ..., num: ... },
    { logfile: ..., num: ... },
    ...
  ],
}
```

Access these as normal with `/get/_kaf?from=…` 


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

Save data using: (HTTP POST)

```
/put/logfile

POSTED Message Data
```

*Example:* `curl localhost:7749 /put/testlog -d @notes`

Get messages using: (HTTP GET)

```
/get/logfile?from=<msg number>
```

Example: `curl localhost:7749 /get/testlog?from=1&format=raw`

Responds with:

```
Message Data
Message Data
```

Or, if you know you have JSON stored: `curl localhost:7749 /get/testlog?from=1&format=json`

Responds with:

```
[{"id":1,"data":"First Record"},
{"id":2,"data":"Second Record"},
...
]
```

If you want the most generic response, simply ask for `kaf` format (or don’t specify the `format` parameter)

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

Disk format is easy to [`cat`](https://en.wikipedia.org/wiki/Cat_(Unix)) /[`tail`](https://en.wikipedia.org/wiki/Tail_(Unix)) / [edit](https://www.vim.org) and examine. You can even open it in your editor and update/fix it easily!

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

## Archival

Sometimes logs can get too big and we don’t need all that old data. We can tell **Kaf** to switch over to a new message log file (the old log file is saved with the name `--name--<datetime>`).

Because we don’t want to have to coordinate across multiple services that may use the log we can ask **Kaf** to copy across some of the latest messages so they are still available.

Request archival of the log using: (HTTP POST)

```
/archive/logfile?upto=<msgnum>
```

Once archived **Kaf** releases any file handles to the log file and you can move it out of the directory or delete it or back it up as you wish.

---

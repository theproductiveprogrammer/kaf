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

## Client

Writing a client for **Kaf** is pretty simple in whatever language you like. Here is a sample client that polls for latest messages in your log in [python](https://python.org):

```python
import sys, time, requests

kafsvr = "http://localhost:7749"
logfile = "mylog"

FROM = 1
while True:
    r = requests.get(f'{kafsvr}/get/{logfile}?from={FROM}&format=raw')
    latest = r.headers.get('x-kaf-lastmsgsent')
    if latest:
        print(r.text, flush=True)
        FROM = int(latest) + 1
    time.sleep(2)
```

To try it out save the python program as `kafclient.py` and put data in your **Kaf** log:

```sh
$> curl localhost:7749/put/mylog -d Test1
$> curl localhost:7749/put/mylog -d Test2
```

Then run the python client:

```sh
$> python3 kafclient.py
Test1
Test2
```

As you add more records:

```sh
$> curl localhost:7749/put/mylog -d Test3
$> curl localhost:7749/put/mylog -d Test4
```

The client will react and print out the new records:

```sh
$> python3 kafclient.py
...
Test3
Test4
```

### Using JSON

You can archive the current log and post JSON data instead:

```sh
$> curl localhost:7749/archive/mylog?upto=100

$> curl localhost:7749/put/mylog -d '{"id":1,"data":"First Record"}'
$> curl localhost:7749/put/mylog -d '{"id":1,"data":"Second Record"}'
```

You can try piping this through [jq](https://stedolan.github.io/jq/):

```sh
$> python kafclient.py | jq
```

And get a pretty JSON output!

---

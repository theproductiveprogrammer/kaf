# Kaf

A Small, High Performance, Event Store (cf. [Apache Kafka](https://kafka.apache.org))

## Why?

When Kakfa is overkill for a your Event Store. 

Also the data format is human-friendly — simple enough to `tail`/`cat`/read with any editor — which helps a lot with understanding your system and quick debugging. You can even edit your data in an editor and **Kaf** will do it’s best to read it correctly.

## How?

Single [golang](https://golang.org) file. Run using:

```sh
$> go run kaf.go <addr> <path to data folder>
```

*Example:* `go run kaf.go 127.0.0.1:7749 ../kaf-data`

## Quickstart

Writing a client for **Kaf** is pretty simple in whatever language you like. Here is a sample client that polls for latest messages in your log in [python](https://python.org):

```python
import sys, time, requests

# let's start from the first message
FROM = 1
while True:
    r = requests.get(f'http://localhost:7749/get/mylog?from={FROM}&format=raw')
    latest = r.headers.get('x-kaf-lastmsgsent')
    if latest:  # got new messages
        print(r.text, flush=True) # show
        FROM = int(latest) + 1    # get next
    time.sleep(1)
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

### Try JSON

Instead of unstructured data like “Test1/Test2” let's put in JSON instead.

First we archive the current log, then post some JSON records:

```sh
$> curl localhost:7749/archive/mylog?upto=100

$> curl localhost:7749/put/mylog -d '{"id":1,"data":"First Record"}'
$> curl localhost:7749/put/mylog -d '{"id":1,"data":"Second Record"}'
```

Now try piping the output we get through [jq](https://stedolan.github.io/jq/):

```sh
$> python kafclient.py | jq
```

As we post JSON data, we will see a pretty JSON output!

## Core API

### Saving Messages to a Logfile

Save data using HTTP POST:

```
/put/logfile

POSTED Message Data
```

*Example:* `curl localhost:7749 /put/testlog -d @notes`

### Getting Messages from the Logfile

Get messages using HTTP GET:

```
/get/logfile?from=<msg number>
```

Example: `curl localhost:7749 /get/testlog?from=1&format=raw`

Responds with:

```
Message Data
Message Data
```

Or, if you have JSON stored:

 `curl localhost:7749 /get/testlog?from=1&format=json`

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

## The Architecture

High performance Golang server - one [goroutine](https://tour.golang.org/concurrency/1) per message log. Uses [synchronous channel](https://tour.golang.org/concurrency/2) for communication. Writes to disk, reads from disk. Uses OS file caching.

Disk format:

```
KAF_DB | v1 | Start Msg Num (\n)
KAF_MSG | Msg Num | Size (\n)
Message Data
...
```

### Human-Friendly Disk format

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

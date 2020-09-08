# Kaf

A Simple Event Store (cf. [Apache Kafka](https://kafka.apache.org))

## Why?

When Kakfa is overkill for a simple Event Store.

## How?

Single, simple [golang](https://golang.org) file. Run using:

```sh
$> go run kaf.go <port> <data folder>
```

Save data using:

```http
/put/logfile

POSTED Message Data
```

Get messages using:

```http
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

## Transparency

Keeps track of information flow:

```http
/stats
```

Responds with

```json
[
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
		stats: { num: ... }
  },

	{
    startTime: <ISO-Format>,
    ...
  }
]
```


# comby-store-sqlite

Implementation of the EventStore and CommandStore interfaces defined in [comby](https://github.com/gradientzero/comby) with SQLite. **comby** is a powerful application framework designed with Event Sourcing and Command Query Responsibility Segregation (CQRS) principles, written in Go.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## Prerequisites

- [Golang 1.22+](https://go.dev/dl/)
- [comby](https://github.com/gradientzero/comby)


## Installation

*comby-store-sqlite* supports the latest version of comby (v2), requires Go version 1.22+ and is based on SQLite driver [modernc.org/sqlite](https://gitlab.com/cznic/sqlite).

```shell
go get github.com/gradientzero/comby-store-sqlite
```

## Quickstart

```go
import (
	"github.com/gradientzero/comby-store-sqlite"
	"github.com/gradientzero/comby/v2"
)

// create sqlite CommandStore
commandStore := store.NewCommandStoreSQLite("./commandStore.db")
if err = commandStore.Init(ctx,
    comby.CommandStoreOptionWithAttribute("anyKey", "anyValue"),
); err != nil {
    panic(err)
}
// create sqlite EventStore
eventStore := store.NewEventStoreSQLite("./eventStore.db")
if err = eventStore.Init(ctx,
    comby.EventStoreOptionWithAttribute("anyKey", "anyValue"),
); err != nil {
    panic(err)
}

// create Facade
fc, _ := comby.NewFacade(
  comby.FacadeWithCommandStore(commandStore),
  comby.FacadeWithEventStore(eventStore),
)
```

## Tests

```shell
go test -v ./...
```

## Contributing
Please follow the guidelines in [CONTRIBUTING.md](./CONTRIBUTING.md).

## License
This project is licensed under the [MIT License](./LICENSE.md).

## Contact
https://www.gradient0.com

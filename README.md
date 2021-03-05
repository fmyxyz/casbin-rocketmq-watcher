# casbin-rocketmq-watcher

[Casbin](https://github.com/casbin/casbin) watcher implementation with rocketmq

## Installation

    go get github.com/fmyxyz/casbin-rocketmq-watcher

## Usage

```go
package main

import (
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/casbin/casbin/v2"
	rocketmqwatcher "github.com/fmyxyz/casbin-rocketmq-watcher"
)

func main() {

	watcher, _ := rocketmqwatcher.NewWatcher(
		[]string{"rocketmqNameServer"},
		"my-policy-topic",
		[]producer.Option{},
		[]consumer.Option{},
	)

	enforcer, _ := casbin.NewSyncedEnforcer("model.conf", "policy.csv")
	enforcer.SetWatcher(watcher)

}
```

## Related pojects
- [Casbin](https://github.com/casbin/casbin)
- [rocketmq-client-go](https://github.com/apache/rocketmq-client-go)


## Additional Usage Examples

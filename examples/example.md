# CASBIN-NATS-WATCHER Example

```go
package main
import (
    "github.com/fmyxyz/rocketmqwatcher"
    "github.com/casbin/casbin/v2"
)

func main() {
    watcher, _ := rocketmqwatcher.NewWatcher(
        []string{"http://nats-endpoint"},
        "my-policy-subject",
        nil,nil,
    )
    
    enforcer := casbin.NewSyncedEnforcer("model.conf", "policy.csv")
    enforcer.SetWatcher(watcher)
}
```

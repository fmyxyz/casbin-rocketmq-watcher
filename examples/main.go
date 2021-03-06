package main

import (
	"github.com/casbin/casbin/v2"
	rocketmqwatcher "github.com/fmyxyz/casbin-rocketmq-watcher"
)

func main() {

	watcher, _ := rocketmqwatcher.NewWatcher()
	enforcer, _ := casbin.NewSyncedEnforcer("model.conf", "policy.csv")
	enforcer.SetWatcher(watcher)

}

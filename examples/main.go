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

package rocketmqwatcher

import (
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/persist"
	"sync"
	"testing"
	"time"
)

// 测试前需要本地启动rocketmq
// 号称史上最便捷搭建RocketMQ服务器的方法:https://my.oschina.net/u/4030990/blog/3232512

var nameServers = []string{"192.168.4.34:9876"}

//var nameServers = []string{"127.0.0.1:9876"}
var policyTopic = "casbin-policy-updated"

var updater persist.Watcher
var listener persist.Watcher

var one sync.Once

func initWatcher() {
	one.Do(func() {

		updater0, err := NewPublishWatcher(nameServers, policyTopic)
		if err != nil {
			panic("Failed to create updater:" + err.Error())
		}
		updater = updater0

		// listener represents any other Casbin enforcer instance that watches the change of policy in DB.
		listener0, err := NewSubscribeWatcher(nameServers, policyTopic, consumer.WithGroupName("casbin"))
		if err != nil {
			panic("Failed to create listener:" + err.Error())
		}
		listener = listener0
		time.Sleep(time.Second * 3)
	})
}

func TestWatcher(t *testing.T) {
	initWatcher()
	chengedCh := make(chan string, 1)

	// listener should set a callback that gets called when policy changes.
	err := listener.SetUpdateCallback(func(msg string) {
		chengedCh <- "listener"
	})
	if err != nil {
		t.Fatal("Failed to set listener callback")
	}
	// updater changes the policy, and sends the notifications.
	err = updater.Update()
	if err != nil {
		t.Fatal("The updater failed to send Update:", err)
	}

	// Validate that listener received message
	var chengedReceived int
	for {
		select {
		case res := <-chengedCh:
			t.Log(res)
			chengedReceived++
		case <-time.After(time.Second * 10):
			t.Fatal("Updater or listener didn't received message in time")
			break
		}
		if chengedReceived == 1 {
			close(chengedCh)
			break
		}
	}
}

func TestWatcherUpdate(t *testing.T) {
	initWatcher()
	// updater changes the policy, and sends the notifications.
	err := updater.Update()
	if err != nil {
		t.Fatal("The updater failed to send Update:", err)
	}

}

func TestWatcherListener(t *testing.T) {
	initWatcher()
	listenerCh := make(chan string, 1)

	// listener should set a callback that gets called when policy changes.
	err := listener.SetUpdateCallback(func(msg string) {
		listenerCh <- "listener"
	})
	if err != nil {
		t.Fatal("Failed to set listener callback")
	}

	// Validate that listener received message
	var listenerReceived bool
	for {
		select {
		case res := <-listenerCh:
			if res != "listener" {
				t.Fatalf("Message from unknown source: %v", res)
				break
			}
			listenerReceived = true
		case <-time.After(time.Second * 10):
			t.Fatal("listener didn't received message in time")
			break
		}
		if listenerReceived {
			close(listenerCh)
			break
		}
	}
}

func TestClose(t *testing.T) {
	initWatcher()
	updater.Close()

	err := updater.Update()
	if err == nil {
		t.Fatal("Closed watcher should return error on update", err)
	}
}

func TestWithEnforcer(t *testing.T) {
	cannel := make(chan string, 1)

	// Initialize the watcher.
	// Use the endpoint of etcd cluster as parameter.
	w, err := NewWatcher(nameServers, policyTopic,
		[]producer.Option{producer.WithInstanceName("TestWithEnforcer")},
		[]consumer.Option{consumer.WithGroupName("casbin"), consumer.WithInstance("TestWithEnforcer")},
	)
	if err != nil {
		t.Fatal("Failed to create updater", err)
	}

	// Initialize the enforcer.
	e, _ := casbin.NewEnforcer("examples/rbac_model.conf", "examples/rbac_policy.csv")

	// Set the watcher for the enforcer.
	e.SetWatcher(w)

	// By default, the watcher's callback is automatically set to the
	// enforcer's LoadPolicy() in the SetWatcher() call.
	// We can change it by explicitly setting a callback.
	w.SetUpdateCallback(func(msg string) {
		cannel <- "enforcer"
	})

	// Update the policy to test the effect.
	e.SavePolicy()

	// Validate that listener received message
	select {
	case res := <-cannel:
		if res != "enforcer" {
			t.Fatalf("Got unexpected message :%v", res)
		}
	case <-time.After(time.Second * 10):
		t.Fatal("The enforcer didn't send message in time")
	}
	close(cannel)

	w.Close()

}

package rocketmqwatcher

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/casbin/casbin/v2/persist"
	"runtime"
	"time"
)

// watcher implements persist.Watcher interface
type watcher struct {
	nameServers        []string
	policyUpdatedTopic string

	producerOpts []producer.Option
	consumerOpts []consumer.Option

	producer rocketmq.Producer
	consumer rocketmq.PushConsumer

	canPub, canSub bool

	callback func(string)

	triggerTime  time.Duration // callback触发周期 最少1s，默认1s
	callbackChan chan string
}

const (
	callbackChanLen = 100
)

// 默认topic:casbin-policy-updated
func NewWatcher(opts ...Option) (persist.Watcher, error) {
	w := &watcher{
		policyUpdatedTopic: "casbin-policy-updated",
		nameServers:        []string{"127.0.0.1:9876"},
		triggerTime:        time.Second, //默认1s
		callbackChan:       make(chan string, callbackChanLen),
	}

	for _, opt := range opts {
		opt(w)
	}

	if w.producer == nil && w.canPub {
		err := w.newProducer()
		if err != nil {
			return nil, err
		}
	}

	if w.consumer == nil && w.canSub {
		err := w.newConsumer()
		if err != nil {
			return nil, err
		}
	}

	if w.consumer != nil {
		err := w.subscribeToUpdates()
		if err != nil {
			return nil, err
		}
	}

	runtime.SetFinalizer(w, finalizer)

	go w.ticker()

	return w, nil
}

func (w *watcher) ticker() {
	defer func() {
		if e := recover(); e != nil {
			fmt.Println("watch ticker : panic ", e)
		}
	}()
	m := make(map[string]struct{})
	ticker := time.NewTicker(w.triggerTime)
	for {
		select {
		case <-ticker.C:
			if w.callback != nil {
				for k := range m {
					fmt.Println("Subscribe trigger:", k)
					w.callback(k)
					delete(m, k)
				}
			}
		case data, ok := <-w.callbackChan:
			if !ok {
				return
			}
			m[data] = struct{}{}
		}
	}
}

// NewWatcher creates new Nats watcher.
// Parameters:
// - nameServers
//		rocketMQ地址
// - policyUpdatedTopic
//      用户通知及订阅更新信息 确保rocketMQ中存在此topic或设置为自动创建topic
//- consumerOpts
//      用于构建消费者
func NewPublishWatcher(nameServers []string, policyUpdatedTopic string, producerOpts ...producer.Option) (persist.Watcher, error) {
	nw := &watcher{
		policyUpdatedTopic: policyUpdatedTopic,
		nameServers:        nameServers,
		producerOpts:       producerOpts,
		canPub:             true,
	}

	err := nw.newProducer()
	if err != nil {
		return nil, err
	}

	runtime.SetFinalizer(nw, finalizer)

	return nw, nil
}

// NewWatcher creates new Nats watcher.
// Parameters:
// - nameServers
//		rocketMQ地址
// - policyUpdatedTopic
//      用户通知及订阅更新信息 确保rocketMQ中存在此topic或设置为自动创建topic
//- consumerOpts
//      用于构建消费者
func NewSubscribeWatcher(nameServers []string, policyUpdatedTopic string, consumerOpts ...consumer.Option) (persist.Watcher, error) {
	nw := &watcher{
		policyUpdatedTopic: policyUpdatedTopic,
		nameServers:        nameServers,
		consumerOpts:       consumerOpts,
		canSub:             true,
	}

	err := nw.newConsumer()
	if err != nil {
		return nil, err
	}

	err = nw.subscribeToUpdates()
	if err != nil {
		return nil, err
	}

	runtime.SetFinalizer(nw, finalizer)

	return nw, nil
}

// SetUpdateCallback sets the callback function that the watcher will call
// when the policy in DB has been changed by other instances.
// A classic callback is Enforcer.LoadPolicy().
func (w *watcher) SetUpdateCallback(callback func(string)) error {
	w.callback = callback
	return nil
}

// Update calls the update callback of other instances to synchronize their policy.
// It is usually called after changing the policy in DB, like Enforcer.SavePolicy(),
// Enforcer.AddPolicy(), Enforcer.RemovePolicy(), etc.
func (w *watcher) Update() error {
	if w.producer != nil {
		mqMsg := primitive.NewMessage(w.policyUpdatedTopic, []byte(""))

		syncResult, err := w.producer.SendSync(context.Background(), mqMsg)
		if err != nil {
			return err
		}
		if syncResult.Status != primitive.SendOK {
			err = fmt.Errorf("未发送成功，status:%d", syncResult.Status)
			return err
		}
		fmt.Println("Send rocketmq Message ID:", syncResult.MsgID)
		return err
	}
	return errors.New("rocketmq producer is nil")
}

// Close stops and releases the watcher, the callback function will not be called any more.
func (w *watcher) Close() {
	finalizer(w)
}

func (w *watcher) newProducer() error {
	var producerOpts []producer.Option
	if len(w.nameServers) != 0 {
		producerOpts = append(producerOpts, producer.WithNameServer(w.nameServers))
	}
	producerOpts = append(producerOpts, w.producerOpts...)
	mqProducer, err := producer.NewDefaultProducer(producerOpts...)
	if err != nil {
		return err
	}
	w.producer = mqProducer
	err = w.producer.Start()
	if err != nil {
		return err
	}
	return nil
}

func (w *watcher) newConsumer() error {
	var consumerOpts []consumer.Option
	if len(w.nameServers) != 0 {
		consumerOpts = append(consumerOpts, consumer.WithNameServer(w.nameServers))
	}
	consumerOpts = append(consumerOpts, w.consumerOpts...)
	mqConsumer, err := consumer.NewPushConsumer(consumerOpts...)
	if err != nil {
		return err
	}
	w.consumer = mqConsumer
	return nil
}

func (w *watcher) subscribeToUpdates() error {
	err := w.consumer.Subscribe(w.policyUpdatedTopic, consumer.MessageSelector{}, func(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		fmt.Println("Subscribe rocketmq Message ID:", ext[0].MsgId)

		// 更新
		w.callbackChan <- string(ext[0].Body)
		//if w.callback != nil {
		//	w.callback(string(ext[0].Body))
		//}

		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return err
	}

	err = w.consumer.Start()
	if err != nil {
		return err
	}
	return nil
}

func finalizer(w *watcher) {

	if w.consumer != nil {
		err := w.consumer.Unsubscribe(w.policyUpdatedTopic)
		if err != nil {
			return
		}
		err = w.consumer.Shutdown()
		if err != nil {
			return
		}
	}
	w.consumer = nil

	if w.producer != nil {
		err := w.producer.Shutdown()
		if err != nil {
			return
		}
	}
	w.producer = nil

	w.callback = nil
	return
}

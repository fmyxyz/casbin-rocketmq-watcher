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
)

// watcher implements persist.Watcher interface
type watcher struct {
	nameServers []string

	producer rocketmq.Producer
	consumer rocketmq.PushConsumer

	policyUpdatedTopic string

	callback func(string)
}

// NewWatcher creates new Nats watcher.
// Parameters:
// - nameServers
//		rocketMQ地址
// - policyUpdatedTopic
//      用户通知及订阅更新信息 确保rocketMQ中存在此topic或设置为自动创建topic
// - producerOpts
//      用于构建生产者
//- consumerOpts
//      用于构建消费者
func NewWatcher(nameServers []string, policyUpdatedTopic string, producerOpts []producer.Option, consumerOpts []consumer.Option) (persist.Watcher, error) {
	nw := &watcher{
		policyUpdatedTopic: policyUpdatedTopic,
		nameServers:        nameServers,
	}

	err := nw.newProducer(producerOpts)
	if err != nil {
		return nil, err
	}

	err = nw.newConsumer(consumerOpts)
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
	}

	err := nw.newProducer(producerOpts)
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
	}

	err := nw.newConsumer(consumerOpts)
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

func (w *watcher) newProducer(producerOpts []producer.Option) error {
	if producerOpts == nil {
		producerOpts = make([]producer.Option, 0, 1)
	}
	producerOpts = append(producerOpts, producer.WithNameServer(w.nameServers))
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

func (w *watcher) newConsumer(consumerOpts []consumer.Option) error {
	if consumerOpts == nil {
		consumerOpts = make([]consumer.Option, 0, 1)
	}
	consumerOpts = append(consumerOpts, consumer.WithNameServer(w.nameServers))
	mqConsumer, err := consumer.NewPushConsumer(consumerOpts...)
	if err != nil {
		return err
	}
	w.consumer = mqConsumer
	return nil
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

func (w *watcher) subscribeToUpdates() error {
	err := w.consumer.Subscribe(w.policyUpdatedTopic, consumer.MessageSelector{}, func(ctx context.Context, ext ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		fmt.Println("Subscribe rocketmq Message ID:", ext[0].MsgId)
		// 更新
		if w.callback != nil {
			w.callback(string(ext[0].Body))
		}

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

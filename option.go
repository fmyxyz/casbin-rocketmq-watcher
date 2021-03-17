package rocketmqwatcher

import (
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

type Option func(*watcher)

// rocketmq nameServers
func WithNameServers(nameServers ...string) Option {
	return func(w *watcher) {
		w.nameServers = nameServers
		w.canSub = true
		w.canPub = true
	}
}

// 默认:casbin-policy-updated
func WithTopic(topic string) Option {
	return func(w *watcher) {
		w.policyUpdatedTopic = topic
	}
}

// 使用此项则 WithProducerOpts 无效
func WithProducer(p rocketmq.Producer) Option {
	return func(w *watcher) {
		w.producer = p
		w.canPub = true
	}
}

// 使用此项则 WithConsumerOpts 无效
func WithConsumer(c rocketmq.PushConsumer) Option {
	return func(w *watcher) {
		w.consumer = c
		w.canSub = true
	}
}

func WithProducerOpts(opts ...producer.Option) Option {
	return func(w *watcher) {
		w.producerOpts = opts
		w.canPub = true
	}
}

func WithConsumerOpts(opts ...consumer.Option) Option {
	return func(w *watcher) {
		w.consumerOpts = opts
		w.canSub = true
	}
}

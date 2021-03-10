package api

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

//MessageContext ...
type MessageContext struct {
	ID     int
	Source string

	SpecVersion string
	Type        string
	//OPTIONAL
	DataContentType string
	DataMediaType   string
	DataSchema      string
	Subject         string
	Time            time.Time
	Extensions      map[string]interface{}
}

//Message ..
type Message struct {
	Context MessageContext
	Key     string
	Data    interface{}
}

//APIError ...
type APIError struct {
	Status int
	Code   string
	Err    error
}

func (e APIError) Error() string {
	return e.Error()
}

//MessageEncodingHandle ...
type MessageEncodingHandle func(MessageHandler, Message, error) (Message, error)

//MessageErrorHandle ...
type MessageErrorHandle func(MessageHandler, Message, error) (Message, error)

//MessageHandler ...
type MessageHandler interface {
	Name() string
	Serve(context.Context, Message) (Message, error)
	Next(MessageHandler) MessageHandler
	Sequence([]MessageHandler, MessageErrorHandle) MessageHandler
	Parallel([]MessageHandler, MessageErrorHandle) MessageHandler
}

//MessageHandleFunc ...
type MessageHandleFunc func(context.Context, Message) (Message, error)

//Serve implement
func (mhf MessageHandleFunc) Serve(ctx context.Context, msg Message) (Message, error) {
	return mhf(ctx, msg)
}

//Name ...
func (mhf MessageHandleFunc) Name() string {
	return fmt.Sprintf("%s(%p)", reflect.TypeOf(mhf), mhf)
}

//Next implement
func (mhf MessageHandleFunc) Next(msgHandler MessageHandler) MessageHandler {
	return MessageHandleFunc(func(ctx context.Context, msg Message) (Message, error) {
		result, err := mhf(ctx, msg)
		if err == nil {
			return msgHandler.Serve(ctx, result)
		}
		return result, err
	})
}

//Sequence ...
func (mhf MessageHandleFunc) Sequence(msgHandlers []MessageHandler, f MessageErrorHandle) MessageHandler {
	return MessageHandleFunc(func(ctx context.Context, msg Message) (Message, error) {
		result, err := mhf(ctx, msg)
		if err != nil {
			return result, err
		}
		for _, msgHandler := range msgHandlers {
			if _, err = msgHandler.Serve(ctx, msg); err != nil {
				if msg, err = f(msgHandler, msg, err); err != nil {
					return msg, err
				}
			}
		}
		return msg, nil
	})
}

//Parallel implement
func (mhf MessageHandleFunc) Parallel(msgHandlers []MessageHandler, f MessageErrorHandle) MessageHandler {
	return MessageHandleFunc(func(ctx context.Context, msg Message) (Message, error) {
		result, err := mhf(ctx, msg)
		if err != nil {
			return result, err
		}

		var wg sync.WaitGroup
		for _, msgHandler := range msgHandlers {
			wg.Add(1)
			go func(h MessageHandler) {
				defer wg.Done()
				if _, err := h.Serve(ctx, msg); err != nil {
					if msg, err = f(h, msg, err); err != nil {
						logrus.WithError(err).Errorln(err)
					}
				}

			}(msgHandler)
		}
		wg.Wait()
		return msg, nil
	})
}

//NamedMessageHandler ...
type NamedMessageHandler struct {
	MessageHandler
	name string
	//handler MessageHandler
}

//NamedHandler ...
func NamedHandler(name string, handler MessageHandler) MessageHandler {
	return &NamedMessageHandler{name: name, MessageHandler: handler}
}

//Name ...
func (ne *NamedMessageHandler) Name() string {
	return ne.name
}

//Serve implement
func (ne *NamedMessageHandler) Serve(ctx context.Context, msg Message) (Message, error) {
	logrus.Debugln(ne.name)
	return ne.MessageHandler.Serve(ctx, msg)
}

//QueueItem ...
type QueueItem struct {
	Message

	ID        int64
	RequestID int64
	Created   int64
}

//Queuer queue interface
//Queue Partition
type Queuer interface {
	Put(bucket string, msgs ...Message) error
	Seek(bucket, partition string, keys []string, offset int64, limit int) ([]*QueueItem, error)
	Delete(bucket, partition string, ids ...int64) error
	Replace(bucket, partition string, id int64, msg Message) error
}

//Partitioner ...
type Partitioner interface {
	Get(time.Time) string
	Next(time.Time) string
	Previous(time.Time) string
}

//OffsetItem ...
type OffsetItem struct {
	SubscriberID string
	Bucket       string
	PartitionKey time.Time
	Offset       int64
	CreatedAt    int64
	UpdatedAt    int64
}

//Offseter ...
type Offseter interface {
	Get(subscriber, bucket string) (item *OffsetItem, err error)
	Save(*OffsetItem) error
}

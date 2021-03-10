package stages

import (
	"context"

	"ezreal.com.cn/ez_pipeline/pkg/api"
)

//NewQueue queue stage
func NewQueue(bucket string, queue api.Queuer) api.MessageHandler {
	return api.MessageHandleFunc(func(ctx context.Context, msg api.Message) (api.Message, error) {
		if err := queue.Put(bucket, msg); err != nil {
			return msg, err
		}
		return msg, nil
	})
}

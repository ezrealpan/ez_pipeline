package stages

import (
	"context"
	"fmt"

	"ezreal.com.cn/ez_pipeline/pkg/api"
)

//NewSimple ...
func NewSimple(h api.MessageHandler) api.MessageHandleFunc {
	return func(ctx context.Context, msg api.Message) (api.Message, error) {
		fmt.Println("[stge1] NewSimple data", msg.Data)
		m, _ := h.Serve(ctx, msg)
		fmt.Printf("m:%+v", m)
		return msg, nil
	}
}

//Last ...
func Last() api.MessageHandleFunc {
	return func(ctx context.Context, msg api.Message) (api.Message, error) {
		//get origin data
		data, ok := msg.Data.(map[string]interface{})
		if !ok {
			return msg, fmt.Errorf("Message.Data is not map[string]interface{}")
		}
		fmt.Println("[stge1] Last data", data)
		return msg, nil
	}
}

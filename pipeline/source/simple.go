package source

import (
	"context"
	"fmt"
	"time"

	"ezreal.com.cn/ez_pipeline/pkg/api"
	"github.com/sirupsen/logrus"
)

//UpdateOffset update offset by record
type UpdateOffset func(map[string]interface{}) error

//NewSimple ...
func NewSimple(update UpdateOffset, h api.MessageHandler, eh api.MessageErrorHandle) func(context.Context) {
	return func(ctx context.Context) {
		var recordC <-chan *sourceRecord
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			default:
				fmt.Println("------------------------")
				recordC, _ = queryAsync(ctx)
				for record := range recordC {
					logrus.WithFields(logrus.Fields{"record": record}).Debugln("source:database: record")
					select {
					case <-ctx.Done():
						return
					default:
						if record.err != nil {
							logrus.WithError(record.err).Errorln("database query failed")
						}

						msg := api.Message{
							Context: api.MessageContext{
								ID:     -1,
								Source: "oracle",
								Type:   "map[string]interface{}",
								Time:   time.Now(),
							},
							Data: record.data,
						}

						if msg, err = h.Serve(ctx, msg); err != nil {
							if msg, err = eh(h, msg, err); err != nil {
								logrus.WithField("message", msg).WithError(err).Errorln("database handle message failed")
							}
						}
						if err = update(record.data); err != nil {
							logrus.WithField("record", record).WithError(err).Errorln("database update offset failed")
						}
					}
				}
			}
		}
	}
}

// sourceRecord ...
type sourceRecord struct {
	data map[string]interface{}
	err  error
}

//queryAsync 异步查询方法，一次性查询所有结果并返回
func queryAsync(ctx context.Context) (<-chan *sourceRecord, error) {
	c := make(chan *sourceRecord)
	go func() {
		defer close(c)
		//测试数据
		time.Sleep(1 * time.Second)
		m := make(map[string]interface{})
		m["simple"] = "simple"
		m["source"] = "source"
		c <- &sourceRecord{
			data: m,
		}
	}()

	return c, nil
}

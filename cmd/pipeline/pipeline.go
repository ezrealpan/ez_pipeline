package pipeline

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"ezreal.com.cn/ez_pipeline/pipeline/source"
	"ezreal.com.cn/ez_pipeline/pipeline/stages"
	"ezreal.com.cn/ez_pipeline/pkg/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var configFile string

func init() {
	CMD.Flags().StringVar(&configFile, "conf", "config.toml", "path config file")
}

var CMD = &cobra.Command{
	Use: "pip",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		last := stages.Last()
		// source -> queue(origin data)
		stageSimple := stages.NewSimple(last)
		// startOriginSource
		start := source.NewSimple(
			updateOffset,
			stageSimple,
			func(h api.MessageHandler, msg api.Message, err error) (api.Message, error) {
				if err != nil {
					logrus.WithFields(logrus.Fields{
						"handler": h.Name(),
						"message": msg,
					}).WithError(err).Errorln("fetch from origin")
				}
				return msg, nil
			},
		)
		//start process
		start(ctx)
		stopChan := make(chan os.Signal, 1)
		signal.Notify(stopChan, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		<-stopChan
	},
}

func updateOffset(map[string]interface{}) error {
	return nil
}

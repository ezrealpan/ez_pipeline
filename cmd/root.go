package cmd

import (
	"ezreal.com.cn/ez_pipeline/cmd/pipeline"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "root",
		Short: "A generator for Cobra based Applications",
		Long: `Cobra is a CLI library for Go that empowers applications.
				This application is a tool to generate the needed files
				to quickly create a Cobra application.
				`,
	}
)

// Execute executes the root command.
func Execute() error {
	//NewServerCmd
	rootCmd.AddCommand(pipeline.CMD)
	return rootCmd.Execute()
}

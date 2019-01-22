package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "flow-tool [command]",
	Short: "flow-tool is a helper utility for flow framework",
	Long:  "flow-tool is a utility for flowd and flow plugins development.",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func Execute() {
	rootCmd.Execute()
}

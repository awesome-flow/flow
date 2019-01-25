package cmd

import (
	"github.com/spf13/cobra"
)

var dockerCmd = &cobra.Command{
	Use:   "docker [command]",
	Short: "Docker dev env related toolkit",
	Run: func(cmd *cobra.Command, args []string) {

	},
}

var dockerComposeCmd = &cobra.Command{
	Use:   "compose",
	Short: "Generates docker-compose file from the pipeline",
	Long: `This command scaffolds a docker-compose file based
		on flow config file. Flow links, including plugins,
		can define their dev environment using docker-compose
		yaml. This definition is being picked up by flow-tool
		and compiled into a single docker-compose file. Links
		can template their definition parameters (names, ports)
		in order to not interfere with their siblings.`,
	Run: func(cmd *cobra.Command, args []string) {},
}

func init() {
	dockerCmd.AddCommand(dockerComposeCmd)
	rootCmd.AddCommand(dockerCmd)
}

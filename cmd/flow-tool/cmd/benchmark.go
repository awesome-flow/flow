package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/awesome-flow/flow/pkg/config"
	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/pipeline"
	"github.com/spf13/cobra"
)

var benchmarkCmd = &cobra.Command{
	Use:   "benchmark [command]",
	Short: "Embeds the component in a test pipeline and benchmarks it",
	// Run: func(cmd *cobra.Command, args []string) {
	// },
}

var linkname string
var linkto int
var routeto string
var options *[]string

var cntr uint64

func reportCnt() {
	v := atomic.SwapUint64(&cntr, 0)
	log.Printf("Counter: %d msg/sec", v)
	time.Sleep(time.Second)
	go reportCnt()
}

type countingsink struct {
	*core.Connector
}

func (*countingsink) Recv(msg *core.Message) error {
	atomic.AddUint64(&cntr, 1)
	return msg.AckDone()
}

var benchmarkLinkCmd = &cobra.Command{
	Use:   "link [link name]",
	Short: "Build a test pipeline with the link and benchmarks it",
	RunE: func(cmd *cobra.Command, args []string) error {

		params := make(map[string]interface{})
		for _, kv := range *options {
			chunks := strings.Split(kv, "=")
			if len(chunks) != 2 {
				return fmt.Errorf("Malformed option %s, expected format: k=v", kv)
			}
			k, v := chunks[0], chunks[1]
			if vi, err := strconv.Atoi(v); err == nil {
				params[k] = vi
			} else {
				params[k] = v
			}
		}

		ppl, err := pipeline.NewPipeline(
			map[string]config.CfgBlockComponent{
				"tcp_rcv": {
					Module:      "receiver.tcp",
					Constructor: "New",
					Params: map[string]interface{}{
						"bind_addr": ":3101",
						"mode":      "talkative",
						"backend":   "std",
					},
				},
				//TODO (olegs): plugin support
				"bench_link": {
					Module:      linkname,
					Constructor: "New",
					Params:      params,
				},
			},
			map[string]config.CfgBlockPipeline{
				"tcp_rcv": {Connect: "bench_link"},
			},
		)
		if err != nil {
			return err
		}

		links := ppl.Links()
		//TODO (olegs): routeto
		if linkto != -1 {
			sinks := make([]core.Link, 0, linkto)
			for i := 0; i < linkto; i++ {
				sinks = append(sinks, &countingsink{core.NewConnector()})
			}
			links[len(links)-1].LinkTo(sinks)
		} else {
			sink := &countingsink{core.NewConnector()}
			links[len(links)-1].ConnectTo(sink)
		}

		go reportCnt()

		if err := ppl.Start(); err != nil {
			return err
		}

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c

		if err := ppl.Stop(); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	benchmarkCmd.AddCommand(benchmarkLinkCmd)
	rootCmd.AddCommand(benchmarkCmd)

	benchmarkLinkCmd.Flags().StringVarP(&linkname, "link", "l", "", "Link name")
	benchmarkLinkCmd.Flags().IntVarP(&linkto, "link-to", "", -1, "")
	options = benchmarkLinkCmd.Flags().StringSliceP("options", "o", []string{}, "Link options")
	benchmarkLinkCmd.MarkFlagRequired("link")
}

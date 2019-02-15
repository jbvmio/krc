package main

import (
	"flag"

	"github.com/jbvmio/kafkactl"

	"github.com/jbvmio/krc"
	"github.com/jbvmio/krc/api"
	"github.com/jbvmio/krc/check"
	"github.com/jbvmio/krc/config"
	krcSync "github.com/jbvmio/krc/sync"
)

var (
	// vars:
	conf config.Config

	// flags:
	cfgFile     = flag.String("config", "./krc.yaml", "Path of config file.")
	showDebug   = flag.Bool("debug", false, "Print Kafka Debug Info.")
	performTest = flag.Bool("t", false, "Parse Config, Perform Test and Exit.")
	showVer     = flag.Bool("v", false, "Show Version Info.")
)

func main() {
	flag.Parse()
	conf = config.GetConfig(*cfgFile)
	if *showVer {
		krc.PrintVersion()
		return
	}
	if *showDebug {
		kafkactl.Logger("[krc] ")
	}
	if *performTest {
		krcSync.PerformTest(&conf)
		return
	}
	krc.PrintVersion()
	krc.ValidateConfig(&conf)
	krcSync.InitialSync(&conf)
	go krcSync.TopicSync(&conf)
	go check.ReplicationCheck(&conf)
	api.InitAPI(&conf)
}

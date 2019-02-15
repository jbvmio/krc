package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jbvmio/krc/ops"
	"github.com/jbvmio/krc/output"

	"github.com/spf13/viper"
)

type Config struct {
	DataDir           string              `yaml:"dataDir"`
	DedicatedTopic    string              `yaml:"dedicatedTopic"`
	CheckInterval     time.Duration       `yaml:"checkInterval"`
	UpdateInterval    time.Duration       `yaml:"updateInterval"`
	RetryInterval     time.Duration       `yaml:"retryInterval"`
	TimeOut           time.Duration       `yaml:"timeout"`
	MinOffset         int64               `yaml:"minOffset"`
	LocalBrokers      []string            `yaml:"localBrokers"`
	RemoteBrokers     []string            `yaml:"remoteBrokers"`
	PeerBrokers       []string            `yaml:"peerBrokers"`
	LocalBoot         string              `yaml:"localBootstrap"`
	RemoteBoot        string              `yaml:"remoteBootstrap"`
	CheckKey          string              `yaml:"checkKey"`
	WRegex            string              `yaml:"whitelistRegex"`
	BRegex            string              `yaml:"blacklistRegex"`
	Whitelist         []string            `yaml:"whitelist"`
	Blacklist         []string            `yaml:"blacklist"`
	APIEndpoint       string              `yaml:"apiEndpoint"`
	ContextDelimiter  string              `yaml:"contextDelimiter"`
	ReplicationChecks []string            `yaml:"replicationChecks"`
	Clusters          map[string][]string `yaml:"clusters"`
}

func GetConfig(cfgFile string) Config {
	if !ops.FileExists(cfgFile) {
		output.Failf("No config file, try again.")
	}
	initViper(cfgFile)
	var conf Config
	conf.DataDir = viper.GetString("dataDir")
	conf.DedicatedTopic = viper.GetString("dedicatedTopic")
	conf.LocalBrokers = viper.GetStringSlice("localBrokers")
	conf.RemoteBrokers = viper.GetStringSlice("remoteBrokers")
	conf.PeerBrokers = viper.GetStringSlice("peerBrokers")
	conf.LocalBoot = viper.GetString("localBootstrap")
	conf.RemoteBoot = viper.GetString("remoteBootstrap")
	conf.CheckKey = viper.GetString("checkKey")
	conf.CheckInterval = viper.GetDuration("checkInterval")
	conf.RetryInterval = viper.GetDuration("retryInterval")
	conf.UpdateInterval = viper.GetDuration("updateInterval")
	conf.TimeOut = viper.GetDuration("timeout")
	conf.MinOffset = viper.GetInt64("minOffset")
	conf.WRegex = viper.GetString("whitelistRegex")
	conf.BRegex = viper.GetString("blacklistRegex")
	conf.Whitelist = viper.GetStringSlice("whitelist")
	conf.Blacklist = viper.GetStringSlice("blacklist")
	conf.APIEndpoint = viper.GetString("apiEndpoint")

	// ToDo when configuring multiple cluster checks
	/*
		conf.ReplicationChecks = viper.GetStringSlice("replicationChecks")
		conf.Clusters = viper.GetStringMapStringSlice("clusters")
	*/

	return conf
}

func getContextMap(contexts []string, delimiter string) map[string][]string {
	ctx := make(map[string][]string)
	for _, c := range contexts {
		clusters := strings.Split(c, delimiter)
		if len(clusters) == 2 {
			ctx[clusters[0]] = viper.GetStringSlice(clusters[0])
			ctx[clusters[1]] = viper.GetStringSlice(clusters[1])
		}
	}
	return ctx
}

func initViper(cfgFile string) {
	viper.SetConfigFile(cfgFile)
	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Error reading config file:", viper.ConfigFileUsed(), err)
		os.Exit(1)
	}
}

func readConfig(path string) []byte {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("Error reading config file: %v\n", err)
	}
	return file
}

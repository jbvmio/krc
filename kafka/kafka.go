package kafka

import (
	"regexp"

	"github.com/jbvmio/krc/config"
	"github.com/jbvmio/krc/ops"
	"github.com/jbvmio/krc/output"

	"github.com/jbvmio/kafkactl"
)

const MaxClusters = 2

type Cluster struct {
	Brokers []string
	Topics  []kafkactl.TopicMeta
}

type TopicMatches struct {
	MatchedTopics []string
	Clusters      [MaxClusters]Cluster
}

func SelectBroker(brokers ...string) string {
	bootStrap, err := kafkactl.ReturnFirstValid(brokers...)
	if err != nil {
		output.Failf("Error obtaining a Valid Broker: %v", err)
	}
	return bootStrap
}

func (tm *TopicMatches) FilterLists(config *config.Config) {
	tm.FilterWhitelists(config)
	tm.FilterBlacklists(config)
	tm.MatchedTopics = ops.FilterUnique(tm.MatchedTopics)
}

func (tm *TopicMatches) FilterWhitelists(config *config.Config) {
	var listMap map[string]bool
	var regex *regexp.Regexp
	var filtered []string
	if len(config.Whitelist) > 0 {
		listMap = make(map[string]bool, len(config.Whitelist))
		for _, item := range config.Whitelist {
			listMap[item] = true
		}
	}
	if config.WRegex != "" {
		regex = regexp.MustCompile(config.WRegex)
	}
	for _, topic := range tm.MatchedTopics {
		if listMap[topic] {
			filtered = append(filtered, topic)
		}
		if regex != nil {
			if !listMap[topic] {
				matches := regex.FindStringSubmatch(topic)
				switch match := len(matches); match {
				case 1:
					filtered = append(filtered, topic)
				case 2:
					filtered = append(filtered, topic)
				}
			}
		}
	}
	tm.MatchedTopics = filtered
}

func (tm *TopicMatches) FilterBlacklists(config *config.Config) {
	var listMap map[string]bool
	var regex *regexp.Regexp
	var filtered []string
	if len(config.Blacklist) > 0 {
		listMap = make(map[string]bool, len(config.Blacklist))
		for _, item := range config.Blacklist {
			listMap[item] = true
		}
	}
	if config.BRegex != "" {
		regex = regexp.MustCompile(config.BRegex)
	}
	for _, topic := range tm.MatchedTopics {
		if !listMap[topic] {
			if regex != nil {
				matches := regex.FindStringSubmatch(topic)
				switch match := len(matches); match {
				case 0:
					filtered = append(filtered, topic)
				}
			}
		}
	}
	tm.MatchedTopics = filtered
}

func SearchPartitionMeta(client *kafkactl.KClient, topic string, partition int32) kafkactl.TopicMeta {
	tMeta, err := client.GetTopicMeta()
	if err != nil {
		return kafkactl.TopicMeta{Topic: "ErrTopicMetadata"}
	}
	for _, m := range tMeta {
		if m.Topic == topic {
			if m.Partition == partition {
				return m
			}
		}
	}
	return kafkactl.TopicMeta{Topic: "NOTFOUND"}
}

func TopicExists(client *kafkactl.KClient, topic string) bool {
	tMeta, err := client.GetTopicMeta()
	if err != nil {
		return false
	}
	for _, m := range tMeta {
		if m.Topic == topic {
			return true
		}
	}
	return false
}

func SearchTopicMeta(client *kafkactl.KClient, topic string) []kafkactl.TopicMeta {
	var matchedMeta []kafkactl.TopicMeta
	tMeta, err := client.GetTopicMeta()
	if err != nil {
		matchedMeta = append(matchedMeta, kafkactl.TopicMeta{Topic: "ErrTopicMetadata"})
		return matchedMeta
	}
	for _, m := range tMeta {
		if m.Topic == topic {
			matchedMeta = append(matchedMeta, m)
		}
	}
	return matchedMeta
}

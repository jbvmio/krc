package krc

import (
	"fmt"
	"strings"
	"sync"

	"github.com/jbvmio/krc/config"
	"github.com/jbvmio/krc/kafka"
	"github.com/jbvmio/krc/output"

	"github.com/jbvmio/kafkactl"
	"github.com/spf13/cast"
)

var DedicatedRecreated bool

func NewTopicCheck(config *config.Config, existingTopics []string) (topics kafka.TopicMatches, oldTopics []string) {
	var newTopics []string
	currentTopics := make(map[string]bool, len(existingTopics))
	missingTopics := make(map[string]bool, len(existingTopics))
	for _, topic := range existingTopics {
		currentTopics[topic] = true
		missingTopics[topic] = true
	}
	ValidateDedicatedTopic(config)
	topics = ProcessTopics(config)
	for _, new := range topics.MatchedTopics {
		if !currentTopics[new] {
			newTopics = append(newTopics, new)
		}
		missingTopics[new] = false
	}
	for k := range missingTopics {
		if missingTopics[k] {
			oldTopics = append(oldTopics, k)
		}
	}
	topics.MatchedTopics = newTopics
	output.Infof("New Topics Matched:")
	output.PrintStrings(topics.MatchedTopics...)
	return
}

func ProcessTopics(config *config.Config) kafka.TopicMatches {
	fmt.Println("Processing Topics")
	topics := matchTopics(config)
	fmt.Println("Filtering Topics")
	topics.FilterLists(config)
	if config.DedicatedTopic != "" {
		topics.MatchedTopics = append(topics.MatchedTopics, config.DedicatedTopic)
	}
	return topics
}

func ProcessNewTopics(config *config.Config, tm kafka.TopicMatches) (details []*Detail) {
	topicList := tm.MatchedTopics
	topicDone := make(map[string]bool, len(topicList))
	for _, topic := range topicList {
		var RP []*ReplicatedPartition
		if !topicDone[topic] {
			topicDone[topic] = true
			var tmpMeta []kafkactl.TopicMeta
			for _, tm := range tm.Clusters[0].Topics {
				if tm.Topic == topic {
					tmpMeta = append(tmpMeta, tm)
				}
			}
			TOM := GetOffsetMap(config.LocalBoot, tmpMeta)
			for _, t := range TOM {
				if t.Topic == topic {
					for partition := range t.PartitionOffsets {
						pc := ReplicatedPartition{
							Topic:          t.Topic,
							LocalBroker:    config.LocalBoot,
							LocalPartition: partition,
							RemoteBroker:   config.RemoteBoot,
						}
						RP = append(RP, &pc)
					}
				}
			}
			InitReplicatedParts(RP)
			status := Status{
				Status: ErrInitializing,
				Topic:  topic,
				State:  ErrInitializingString,
			}
			detail := Detail{
				Summary:              status,
				ReplicatedPartitions: RP,
			}
			details = append(details, &detail)
		}
	}
	return
}

func matchTopics(config *config.Config) kafka.TopicMatches {
	var matchedTopics []string
	clusters := getClusters(config.LocalBoot, config.RemoteBoot)
	topicMatch := make(map[string]bool)
	if len(clusters) > 2 {
		output.Failf("To many clusters encountered, Max = 2")
	}
	for _, c1 := range clusters[0].Topics {
		if !topicMatch[c1.Topic] {
			topicMatch[c1.Topic] = true
		}
	}
	for _, c2 := range clusters[1].Topics {
		if topicMatch[c2.Topic] {
			matchedTopics = append(matchedTopics, c2.Topic)
		}
	}
	tm := kafka.TopicMatches{
		MatchedTopics: matchedTopics,
	}
	if containsBroker(config.LocalBoot, clusters[0]) {
		tm.Clusters[0] = clusters[0]
		tm.Clusters[1] = clusters[1]
	} else {
		tm.Clusters[0] = clusters[1]
		tm.Clusters[1] = clusters[0]
	}
	return tm
}

func getClusters(brokers ...string) []kafka.Cluster {
	clusters := make([]kafka.Cluster, 0, kafka.MaxClusters)
	clusterChan := make(chan kafka.Cluster, len(brokers))
	errChan := make(chan error, len(brokers))
	var errFound error
	var wg sync.WaitGroup
	for _, b := range brokers {
		cl, err := kafkactl.NewClient(b)
		if err != nil {
			output.Failf("Error: %v", err)
		}
		wg.Add(1)
		go func(client *kafkactl.KClient, br string) {
			wg.Done()
			fmt.Println(br, "- Obtaining Metadata")
			bList, err := client.BrokerList()
			if err != nil {
				errChan <- err
				return
			}
			fmt.Println(br, "- Obtaining Topics")
			tMeta, err := client.GetTopicMeta()
			if err != nil {
				errChan <- err
				return
			}
			fmt.Println(br, "- Obtaining Offsets")
			cluster := kafka.Cluster{
				Brokers: bList,
				Topics:  tMeta,
			}
			fmt.Println(br, "- Sending Metadata")
			clusterChan <- cluster
			client.Close()
		}(cl, b)
		wg.Wait()
	}
	for i := 0; i < len(brokers); i++ {
		select {
		case cluster := <-clusterChan:
			clusters = append(clusters, cluster)
		case err := <-errChan:
			errFound = err
		}
	}
	if errFound != nil {
		output.Failf("Error: %v", errFound)
	}
	fmt.Println("Metadata Request Complete.")
	return clusters
}

func containsBroker(broker string, cluster kafka.Cluster) bool {
	for _, b := range cluster.Brokers {
		if strings.Contains(b, broker) {
			return true
		}
	}
	return false
}

func ValidateDedicatedTopic(config *config.Config) {
	if config.DedicatedTopic != "" {
		fmt.Println("Validating Dedicated Topic")
		if good := validateDedicated(config); !good {
			output.Failf("Error Creating Dedicated Topic.")
		}
	}
}

func validateDedicated(config *config.Config) bool {
	var created bool
	var restartCG bool
	if config.DedicatedTopic != "" {
		client, err := kafkactl.NewClient(config.LocalBoot)
		if err != nil {
			output.Warnf("%v", err)
			return false
		}
		exists := kafka.TopicExists(client, config.DedicatedTopic)
		if !exists {
			var rf int16 = 3
			bList, _ := client.BrokerList()
			if len(bList) < 3 {
				rf = cast.ToInt16(len(bList))
			}
			err = client.AddTopic(config.DedicatedTopic, cast.ToInt32(len(bList)), rf)
			if err != nil {
				if !strings.Contains(err.Error(), `Topic with this name already exists`) {
					output.Warnf("%v", err)
					client.Close()
					return false
				} else {
					output.Infof("%v: Topic Exists", config.LocalBoot)
				}
			}
			output.Infof("%v: %v Topic Created", config.LocalBoot, config.DedicatedTopic)
			created = true
			restartCG = true
		} else {
			output.Infof("%v: %v Topic Exists", config.LocalBoot, config.DedicatedTopic)
		}
		if created {
			for !exists {
				exists = kafka.TopicExists(client, config.DedicatedTopic)
			}
			created = false
		}
		client.Close()

		client, err = kafkactl.NewClient(config.RemoteBoot)
		if err != nil {
			output.Warnf("%v", err)
			return false
		}
		exists = kafka.TopicExists(client, config.DedicatedTopic)
		if !exists {
			var rf int16 = 3
			bList, _ := client.BrokerList()
			if len(bList) < 3 {
				rf = cast.ToInt16(len(bList))
			}
			err = client.AddTopic(config.DedicatedTopic, cast.ToInt32(len(bList)), rf)
			if err != nil {
				if !strings.Contains(err.Error(), `Topic with this name already exists`) {
					output.Warnf("%v", err)
					client.Close()
					return false
				} else {
					output.Infof("%v: Topic Exists", config.RemoteBoot)
				}
			}
			output.Infof("%v: %v Topic Created", config.RemoteBoot, config.DedicatedTopic)
			created = true
			restartCG = true
		} else {
			output.Infof("%v: %v Topic Exists", config.RemoteBoot, config.DedicatedTopic)
		}
		if created {
			for !exists {
				exists = kafka.TopicExists(client, config.DedicatedTopic)
			}
		}
		client.Close()
		if restartCG {
			output.Infof("Dedicated Topic Has been Re-Created, CG needs restarting.")
			DedicatedRecreated = true
		}
	}
	return true
}

func GetOffsetMap(bootStrap string, topicMeta []kafkactl.TopicMeta) []kafkactl.TopicOffsetMap {
	client, err := kafkactl.NewClient(bootStrap)
	if err != nil {
		output.Failf("Error: %v", err)
	}
	tom := client.MakeTopicOffsetMap(topicMeta)
	client.Close()
	return tom
}

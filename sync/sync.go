package sync

import (
	"fmt"
	"sync"
	"time"

	"github.com/jbvmio/krc"
	"github.com/jbvmio/krc/config"
	"github.com/jbvmio/krc/db"
	"github.com/jbvmio/krc/output"

	"github.com/jbvmio/kafkactl"
)

var (
	inProgress bool
	sl         *syncLock
)

func PerformTest(config *config.Config) {
	krc.PrintVersion()
	output.Infof("Performing Test ... \n")
	output.Infof("Config Details:\n")
	output.PrintObject(config, "yaml")

	krc.ValidateConfig(config)
	m := krc.ProcessTopics(config)
	topicList := m.MatchedTopics
	output.Infof("\nTopics Matched:")
	output.PrintStrings(topicList...)
	output.Infof("")
}

func InitialSync(config *config.Config) {
	fmt.Printf("Starting Initial Sync ... \n")
	krc.ValidateDedicatedTopic(config)
	krc.StopChannel = make(chan bool, krc.StopCount)
	sl = &syncLock{
		inProgress: false,
		mutex:      &sync.Mutex{},
	}
	m := krc.ProcessTopics(config)
	topicList := m.MatchedTopics
	if len(topicList) < 1 {
		output.Failf("No Topics Matched, Nothing to Check.")
	}
	output.Infof("Topics Matched:")
	output.PrintStrings(topicList...)
	var details []*krc.Detail
	topicDone := make(map[string]bool, len(topicList))
	for _, topic := range topicList {
		var RP []*krc.ReplicatedPartition
		if !topicDone[topic] {
			topicDone[topic] = true
			var tmpMeta []kafkactl.TopicMeta
			for _, tm := range m.Clusters[0].Topics {
				if tm.Topic == topic {
					tmpMeta = append(tmpMeta, tm)
				}
			}
			TOM := krc.GetOffsetMap(config.LocalBoot, tmpMeta)
			for _, t := range TOM {
				if t.Topic == topic {
					for partition := range t.PartitionOffsets {
						pc := krc.ReplicatedPartition{
							Kind:           krc.LocalKind,
							Topic:          t.Topic,
							LocalBroker:    config.LocalBoot,
							LocalPartition: partition,
							RemoteBroker:   config.RemoteBoot,
						}
						RP = append(RP, &pc)
					}
				}
			}
		}
		krc.InitReplicatedParts(RP)
		status := krc.Status{
			Status: krc.ErrInitializing,
			Topic:  topic,
			State:  krc.ErrInitializingString,
		}
		detail := krc.Detail{
			Summary:              status,
			ReplicatedPartitions: RP,
		}
		details = append(details, &detail)
	}
	db.SaveTopics(config, details...)
	db.SaveTopicList(config, topicList)
}

func TopicSync(config *config.Config) {
	fmt.Printf("Launching Topic Sync Process ... \n")
	db.MemoryStore = make(map[string]string)
	db.MemoryTopicList = db.GetTopicList(config)
	tops := db.GetTopics(config, db.MemoryTopicList...)
	for _, t := range tops {
		db.MemoryStore[t.Topic] = t.Details
	}
	timer := make(chan bool, 1)
	go func() {
		for {
			time.Sleep(config.UpdateInterval)
			timer <- true
		}
	}()
SyncLoop:
	for {
		select {
		case <-timer:
			TopicSyncCheck(config)
		case s := <-krc.StopChannel:
			if s {
				break SyncLoop
			}
		}
	}
	output.Infof("Topic Sync Process Stopped.")
}

func TopicSyncCheck(config *config.Config) {
	if !sl.inProgress {
		sl.lockSync()
		newTopics, oldTopics := krc.NewTopicCheck(config, db.MemoryTopicList)
		if len(newTopics.MatchedTopics) > 0 {
			db.MemoryTopicList = append(db.MemoryTopicList, newTopics.MatchedTopics...)
			new := krc.ProcessNewTopics(config, newTopics)
			db.SaveTopics(config, new...)
			db.SaveTopicList(config, db.MemoryTopicList)
		}
		if len(oldTopics) > 0 {
			db.RemoveFromTopicList(config, oldTopics...)
		}
		sl.unlockSync()
	}
}

type syncLock struct {
	inProgress bool
	mutex      *sync.Mutex
}

func (sl *syncLock) lockSync() {
	sl.mutex.Lock()
	sl.inProgress = true
}

func (sl *syncLock) unlockSync() {
	sl.inProgress = false
	sl.mutex.Unlock()
}

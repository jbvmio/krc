package check

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jbvmio/krc"
	"github.com/jbvmio/krc/config"
	"github.com/jbvmio/krc/db"
	"github.com/jbvmio/krc/output"
	krcSync "github.com/jbvmio/krc/sync"

	"github.com/jbvmio/kafkactl"
	"github.com/tidwall/pretty"
)

var restartCG chan bool
var getXTopicsChan chan string
var cgStopped = true
var checksRun uint32

func TotalChecksRun() uint32 {
	return checksRun
}

func ReplicationCheck(config *config.Config) {
	output.Infof("Launching Replication Check Process ... ")
	var firstRun = true
	timer := make(chan bool, 1)
	restartCG = make(chan bool, 1)
	getXTopicsChan = make(chan string)
	StartPeerCG(config)
	for cgStopped {
		time.Sleep(time.Millisecond * 300)
	}
	go func() {
		for {
			time.Sleep(config.CheckInterval)
			timer <- true
		}
	}()
CheckLoop:
	for {
		if firstRun {
			firstRun = false
			RunCheck(config)
		}
		select {
		case <-timer:
			krc.ValidateDedicatedTopic(config)
			checkCGRestart(config)
			RunCheck(config)
		case s := <-krc.StopChannel:
			if s {
				break CheckLoop
			}
		}
	}
	output.Infof("Replication Check Process Stopped.")
}

func RunCheck(config *config.Config, topicList ...string) {
	var wg sync.WaitGroup
	var needSync bool
	var details []*krc.Detail
	if len(topicList) < 1 {
		topicList = db.GetTopicList(config)
	}
	topics := db.GetTopics(config, topicList...)
	for _, top := range topics {
		var needRetry bool
		output.Infof("Performing Replication Check for Topic %v ... ", top.Topic)
		var D krc.Detail
		json.Unmarshal([]byte(top.Details), &D)
		for _, rp := range D.ReplicatedPartitions {
			wg.Add(1)
			go rp.RPSendCheck(config, &wg)
		}
		wg.Wait()
		wg.Add(1)
		krc.RPCheck(config, &D, &wg)
		wg.Wait()
		for _, rp := range D.ReplicatedPartitions {
			if rp.Status == 5 {
				needSync = true
			}
			if rp.Status >= D.Summary.Status {
				D.Summary.Status = rp.Status
				D.Summary.State = rp.Result
			}
			if !needRetry {
				if rp.Status != 2 {
					needRetry = true
				}
			}
		}
		details = append(details, &D)
		output.Infof("Results for Topic %v: %v", top.Topic, D.Summary.State)
		if needRetry {
			output.Infof("%v for Topic %v, retrying in %v", D.Summary.State, top.Topic, config.RetryInterval)
			go retryCheck(config, top.Topic)
		}
	}
	db.SaveTopics(config, details...)
	db.RefreshMemStore(config)
	if needSync {
		go krcSync.TopicSyncCheck(config)
	}
	checksRun++
}

func retryCheck(config *config.Config, topic string) {
	krc.ValidateDedicatedTopic(config)
	checkCGRestart(config)
	time.Sleep(config.RetryInterval)
	output.Infof("Performing retry for Topic %v", topic)
	RunCheck(config, topic)
}

func StartPeerCG(config *config.Config) bool {
	go peerCollectCG(config)
	for cgStopped {
		time.Sleep(time.Millisecond * 300)
	}
	return cgStopped
}

func StopPeerCG() bool {
	restartCG <- true
	for !cgStopped {
		time.Sleep(time.Millisecond * 300)
	}
	return cgStopped
}

func GetXTopics(topic string) string {
	getXTopicsChan <- topic
	return <-getXTopicsChan
}

func peerCollectCG(config *config.Config) {
	output.Infof("Starting Peer Consumer Group ... ")
	topics := db.GetTopicList(config)
	client, err := kafkactl.NewClient(config.RemoteBoot)
	if err != nil {
		output.Failf("Error: %v", err)
	}
	consumer, err := client.NewConsumerGroup(config.CheckKey, false, topics...)
	if err != nil {
		output.Failf("Error: %v", err)
	}
	var commitErr error
	var collectorReady = false
	collectChan := make(chan *kafkactl.Message, 100)
	restartChan := make(chan int, 1)
	go collectPeerData(config, collectChan, restartChan, &collectorReady)
	for {
		if collectorReady {
			break
		} else {
			time.Sleep(time.Millisecond * 300)
		}
	}
ConsumerLoop:
	for {
		select {
		case part, ok := <-consumer.CG().Partitions():
			if !ok {
				fmt.Println("ERROR ON MSG CHAN")
				break ConsumerLoop
			}
			go consumer.ConsumeMessages(part, func(msg *kafkactl.Message) bool {
				recKey := fmt.Sprintf("%s", msg.Key)
				if strings.Contains(recKey, config.CheckKey) {
					//output.Infof("Received Match for Topic: %v Partition: %v Offset: %v", msg.Topic, msg.Partition, msg.Offset)
					// timeNow := time.Now()
					// Play with This:> if timeNow.Sub(msg.Timestamp) <= config.CheckInterval {}
					collectChan <- msg
					return true
				}
				//output.Infof("Skipping MSG (No Match) for Topic: %v Partition: %v Offset: %v", msg.Topic, msg.Partition, msg.Offset)
				return true
			})
			if cgStopped {
				cgStopped = false
			}
		case s := <-restartCG:
			if s {
				commitErr = consumer.CG().CommitOffsets()
				restartChan <- 1
				collectorReady = false
				break ConsumerLoop
			}
		case s := <-krc.StopChannel:
			if s {
				commitErr = consumer.CG().CommitOffsets()
				break ConsumerLoop
			}
		}
	}
	for !collectorReady {
		time.Sleep(time.Millisecond * 300)
	}
	if commitErr != nil {
		output.Infof("Warning > Offset Commit Error:", commitErr)
	}
	if err = consumer.Close(); err != nil {
		output.Infof("Error closing Consumer: %v", err)
	}
	if err = client.Close(); err != nil {
		output.Infof("Error closing Client: %v", err)
	}
	output.Infof("Peer Consumer Group Stopped.")
	cgStopped = true
}

func getBrokerType(config *config.Config, broker string) string {
	for _, b := range config.LocalBrokers {
		if strings.Contains(b, broker) {
			return "local_"
		}
	}
	for _, b := range config.PeerBrokers {
		if strings.Contains(b, broker) {
			return "peer_"
		}
	}
	return "unknown_"
}

func collectPeerData(config *config.Config, collectChan chan *kafkactl.Message, restartChan chan int, ready *bool) {
	output.Infof("Starting Peer Collection ... ")
	topicCheckTime := make(map[string]int64)
	topicCheckData := make(map[string]krc.PeerData)
	topicCheckParts := make(map[string]int)
	topicCheckDupe := make(map[string]int)
	*ready = true
CollectLoop:
	for {
		select {
		case msg := <-collectChan:
			src := krc.Source{}
			json.Unmarshal(msg.Value, &src)
			bType := getBrokerType(config, src.LocalBroker)
			topic := string(bType + msg.Topic)
			if topicCheckTime[topic] == 0 {
				topicCheckTime[topic] = time.Now().Unix() - int64(config.CheckInterval.Seconds())
			}
			if src.DateTimeSecs >= topicCheckTime[topic] {
				var dup bool
				topicCheckTime[topic] = src.DateTimeSecs
				top := topicCheckData[topic].Topic
				srcs := topicCheckData[topic].Sources
				if top == "" {
					top = topic
				}
				if topicCheckDupe[topic] == 7777 {
					replacePeerSrc(srcs, src)
				} else {
					srcs = append(srcs, src)
					srcs, dup = filterPeerSources(srcs)
					if dup {
						topicCheckDupe[topic]++
					}
				}
				pd := krc.PeerData{
					Topic:   top,
					Sources: srcs,
				}
				topicCheckData[topic] = pd
			}
			if topicCheckDupe[topic] != 7777 {
				if topicCheckParts[topic] < len(topicCheckData[topic].Sources) {
					topicCheckParts[topic] = len(topicCheckData[topic].Sources)
				}
				if topicCheckDupe[topic] == 1 && len(topicCheckData[topic].Sources) == topicCheckParts[topic] {
					finalSlice := finalizePeerSlice(topicCheckData[topic].Sources)
					pd := krc.PeerData{
						Topic:   topic,
						Sources: finalSlice,
					}
					topicCheckData[topic] = pd
					topicCheckDupe[topic] = 7777
					output.Infof("Finalizing Peer Partition Collection for %v with %v partitions", msg.Topic, len(finalSlice))
				}
			}
		case topic := <-getXTopicsChan:
			switch topic {
			case "alltopics":
				getXTopicsChan <- getXAllTopics(topicCheckData)
			case "statuscheck":
				getXTopicsChan <- getXStatus(topicCheckData, config.CheckInterval)
			case "resetpartitions":
				output.Infof("Received Reset Partitions Request...")
				for k := range topicCheckDupe {
					topicCheckDupe[k] = 0
					topicCheckParts[k] = 0
					topicCheckTime[k] = 0
				}
				getXTopicsChan <- `{"action": "reset partitions"}`
			default:
				getXTopicsChan <- getXTopic(topic, topicCheckData)
			}
		case <-restartChan:
			output.Infof("Restarting Peer Collection...")
			break CollectLoop
		case s := <-krc.StopChannel:
			if s {
				break CollectLoop
			}
		}
	}
	if !*ready {
		*ready = true
	}
	output.Infof("Peer Collection Stopped.")
}

func filterPeerSources(srcs []krc.Source) ([]krc.Source, bool) {
	var topTime int64
	var tmpSrcs []krc.Source
	var duplicate bool
	partMap := make(map[int32]bool)
	for _, src := range srcs {
		if src.DateTimeSecs >= topTime {
			topTime = src.DateTimeSecs
		}
		if !duplicate {
			if !partMap[src.LocalPartition] {
				partMap[src.LocalPartition] = true
			} else {
				duplicate = true
			}
		}
	}
	for _, src := range srcs {
		if src.DateTimeSecs >= topTime {
			tmpSrcs = append(tmpSrcs, src)
		}
	}
	return tmpSrcs, duplicate
}

func finalizePeerSlice(srcs []krc.Source) []krc.Source {
	tmpSrcs := make([]krc.Source, len(srcs))
	for _, src := range srcs {
		tmpSrcs[src.LocalPartition] = src
	}
	return tmpSrcs
}

func replacePeerSrc(srcs []krc.Source, src krc.Source) {
	srcs[src.LocalPartition] = src
}

func getXAllTopics(topicCheckData map[string]krc.PeerData) string {
	var sources []krc.Source
	for k := range topicCheckData {
		sources = append(sources, topicCheckData[k].Sources...)
	}
	j, err := json.Marshal(sources)
	if err != nil {
		return fmt.Sprintf("ErrMarshalSources")
	}
	return fmt.Sprintf("%s", pretty.Pretty(j))
}

func getXTopic(topic string, topicCheckData map[string]krc.PeerData) string {
	var sources []krc.Source
	for k := range topicCheckData {
		if strings.Contains(k, topic) {
			sources = append(sources, topicCheckData[k].Sources...)
		}
	}
	if len(sources) < 1 {
		output.Infof("%v: Error topic not found", topic)
		return fmt.Sprintf(`{"%v": "Err topic not found"}`, topic)
	}
	j, err := json.Marshal(sources)
	if err != nil {
		return fmt.Sprintf("ErrMarshalSources")
	}
	return fmt.Sprintf("%s", pretty.Pretty(j))
}

func getXStatus(topicCheckData map[string]krc.PeerData, checkInterval time.Duration) string {
	const bufferTime = 15
	timeNow := time.Now()
	var PS []krc.PeerStatus
	for k := range topicCheckData {
		ps := krc.PeerStatus{
			Kind: k,
		}
		var topTime int64
		var lowTime int64
		for _, t := range topicCheckData[k].Sources {
			if t.DateTimeSecs > topTime {
				topTime = t.DateTimeSecs
			}
			if t.DateTimeSecs <= lowTime || lowTime == 0 {
				lowTime = t.DateTimeSecs
			}
			if ps.LocalBroker == "" {
				ps.LocalBroker = t.LocalBroker
			}
			if ps.Topic == "" {
				ps.Topic = t.Topic
			}
		}
		ps.SyncDifferenceSecs = topTime - lowTime
		ps.LastSeenSecsAgo = timeNow.Unix() - lowTime
		ps.Partitions = len(topicCheckData[k].Sources)
		if ps.SyncDifferenceSecs <= bufferTime && (float64(ps.LastSeenSecsAgo)-bufferTime) < checkInterval.Seconds() {
			ps.Status = 2
			ps.State = "InSync"
		} else {
			ps.Status = 99
			ps.State = "OutOfSync"
		}
		PS = append(PS, ps)
	}
	j, err := json.Marshal(PS)
	if err != nil {
		return fmt.Sprintf("ErrMarshalSources")
	}
	return fmt.Sprintf("%s", pretty.Pretty(j))
}

func checkCGRestart(config *config.Config) {
	if krc.DedicatedRecreated {
		stopped := StopPeerCG()
		if stopped {
			output.Infof("CG Shutdown Complete.")
			StartPeerCG(config)
		}
		krc.DedicatedRecreated = false
	}
}

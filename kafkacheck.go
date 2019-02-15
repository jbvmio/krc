package krc

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jbvmio/kafkactl"
	"github.com/jbvmio/krc/config"
	"github.com/jbvmio/krc/kafka"
	"github.com/spf13/cast"
)

func ValidateConfig(config *config.Config) {
	if config.LocalBoot == "" {
		config.LocalBoot = kafka.SelectBroker(config.LocalBrokers...)
	}
	if config.RemoteBoot == "" {
		config.RemoteBoot = kafka.SelectBroker(config.RemoteBrokers...)
	}
}

func InitReplicatedParts(RP []*ReplicatedPartition) error {
	var wg sync.WaitGroup
	for _, rp := range RP {
		wg.Add(1)
		go rp.InitReplicatedPartition(&wg)
	}
	wg.Wait()
	return nil
}

func StripMeta(rParts []*ReplicatedPartition) {
	var wg sync.WaitGroup
	for _, rp := range rParts {
		wg.Add(1)
		rp.StripMetadata(&wg)
	}
	wg.Wait()
}

func (rp *ReplicatedPartition) StripMetadata(wg *sync.WaitGroup) {
	rp.RemoteMetadata = kafkactl.TopicOffsetMap{}
	wg.Done()
}

func (rp *ReplicatedPartition) InitReplicatedPartition(wg *sync.WaitGroup) {
	rp.DateTimeSecs = time.Now().Unix()
	rp.Result = ErrInitializingString
	rp.Status = ErrInitializing
	client, errd := kafkactl.NewClient(rp.LocalBroker)
	if errd != nil {
		rp.Result = ErrCreateClientString
		rp.Status = ErrCreateClient
	} else {
		topicMeta := kafka.SearchPartitionMeta(client, rp.Topic, rp.LocalPartition)
		if topicMeta.Topic == "NOTFOUND" {
			rp.Result = ErrTopicOrPartNotFoundString
			rp.Status = ErrTopicOrPartNotFound
		}
		if topicMeta.Topic == "ErrTopicMetadata" {
			rp.Result = ErrTopicMetaString
			rp.Status = ErrTopicMeta
		}
		rp.Metadata = topicMeta
	}
	client.Close()
	wg.Done()
}

func (rp *ReplicatedPartition) RPSendCheck(config *config.Config, wg *sync.WaitGroup) {
	var processErr bool
	rp.DateTimeSecs = time.Now().Unix()
	rp.PreviousResult = rp.Result
	rp.PreviousStatus = rp.Status
	rp.LocalNeedPRE = 0
	rp.RemoteNeedPRE = 0

	client, errd := kafkactl.NewClient(rp.LocalBroker)
	if errd != nil {
		rp.Result = ErrCreateClientString
		rp.Status = ErrCreateClient
		processErr = true
	}
	remoteClient, errd := kafkactl.NewClient(rp.RemoteBroker)
	if errd != nil {
		rp.Result = ErrCreateClientString
		rp.Status = ErrCreateClient
		processErr = true
	}
	if !processErr {
		topicMeta := kafka.SearchPartitionMeta(client, rp.Topic, rp.LocalPartition)
		if topicMeta.Topic == "NOTFOUND" {
			rp.Result = ErrTopicOrPartNotFoundString
			rp.Status = ErrTopicOrPartNotFound
			processErr = true
		}
		if topicMeta.Topic == "ErrTopicMetadata" {
			rp.Result = ErrTopicMetaString
			rp.Status = ErrTopicMeta
			processErr = true
		}
		remoteMeta := kafka.SearchTopicMeta(remoteClient, rp.Topic)
		if len(remoteMeta) < 1 {
			rp.Result = ErrTopicOrPartNotFoundString
			rp.Status = ErrTopicOrPartNotFound
			processErr = true
		}
		if !processErr {
			rp.Metadata = topicMeta
			client.SaramaConfig().Producer.RequiredAcks = sarama.WaitForAll
			client.SaramaConfig().Producer.Return.Successes = true
			client.SaramaConfig().Producer.Return.Errors = true
			client.SaramaConfig().Producer.Partitioner = sarama.NewManualPartitioner
			xPart := (cast.ToInt64(rp.LocalPartition) + 1)
			timeCheck := []byte(fmt.Sprintf("%v_%v", rp.Topic, (rp.DateTimeSecs * xPart)))
			setKey := []byte(config.CheckKey)
			setKey = append(setKey, timeCheck...)
			fs, _ := json.Marshal(&Source{
				Kind:           RemoteKind,
				DateTimeSecs:   rp.DateTimeSecs,
				SetKey:         fmt.Sprintf("%s", setKey),
				Topic:          rp.Topic,
				LocalBroker:    rp.LocalBroker,
				LocalPartition: rp.LocalPartition,
			})
			setVal := fs
			ROM := remoteClient.MakeTopicOffsetMap(remoteMeta)
			for _, r := range ROM {
				if r.Topic == rp.Topic {
					rp.RemoteMetadata = r
				}
			}
			if topicMeta.Leader != topicMeta.Replicas[0] {
				rp.LocalNeedPRE = NeedLocalPRE
			}
			if needPRECheck(remoteMeta) {
				rp.RemoteNeedPRE = NeedRemotePRE
			}
			sendMsg := kafkactl.Message{
				Key:       setKey,
				Value:     setVal,
				Topic:     rp.Topic,
				Partition: rp.LocalPartition,
			}
			rp.SetKey = fmt.Sprintf("%s", sendMsg.Key)
			rp.SetValue = fmt.Sprintf("%s", sendMsg.Value)
			part, off, err := client.SendMSG(&sendMsg)
			rp.LocalOffset = off
			if part != rp.LocalPartition {
				rp.Status = ErrPartitionOrOffset
				rp.Result = ErrPartitionOrOffsetString
				processErr = true
			}
			if off < 0 {
				rp.Status = ErrPartitionOrOffset
				rp.Result = ErrPartitionOrOffsetString
				processErr = true
			}
			if err != nil {
				rp.Status = ErrSendMsg
				rp.Result = ErrSendMsgString
				processErr = true
			}
			if !processErr {
				msg, _ := client.ConsumeOffsetMsg(rp.Topic, part, off)
				rp.LocalDateTimeNano = msg.Timestamp.UnixNano()
			}
		}
	}
	rp.GetValue = ""
	rp.Match = false
	client.Close()
	remoteClient.Close()
	wg.Done()
}

func RPCheck(config *config.Config, D *Detail, wg *sync.WaitGroup) {
	var result string
	var status int16
	var processErr bool
	remoteClient, errd := kafkactl.NewClient(config.RemoteBoot)
	if errd != nil {
		result = ErrCreateClientString
		status = ErrCreateClient
		processErr = true
	}
	if !processErr {
		topic, keyValMap, remoteOffsetMap := getMaps(D)
		keyFoundMap := make(map[string]bool)
		msgChan := make(chan *kafkactl.Message, 100)
		stopChan := make(chan bool, len(remoteOffsetMap))
		for part, offset := range remoteOffsetMap {
			rel := (offset - 1)
			go remoteClient.ChanPartitionConsume(topic, part, rel, msgChan, stopChan)
		}
		timer := make(chan string, 1)
		go func() {
			time.Sleep(config.TimeOut)
			timer <- ErrTimedOutString
		}()
		var count int
	ConsumeLoop:
		for {
			if count >= len(remoteOffsetMap) {
				break ConsumeLoop
			}
			select {
			case msg := <-msgChan:
				recKey := fmt.Sprintf("%s", msg.Key)
				recVal := fmt.Sprintf("%s", msg.Value)
				if keyValMap[recKey] == recVal {
					if !keyFoundMap[recKey] {
						for _, rp := range D.ReplicatedPartitions {
							if rp.SetKey == recKey {
								keyFoundMap[recKey] = true
								rp.RemoteDateTimeNano = msg.Timestamp.UnixNano()
								rp.LatencyNano = rp.RemoteDateTimeNano - rp.LocalDateTimeNano
								rp.LatencyMS = float32(rp.LatencyNano) / 1000000
								rp.LastSuccessDateTimeSecs = msg.Timestamp.Unix()
								rp.GetValue = recVal
								rp.RemotePartition = msg.Partition
								rp.RemoteOffset = msg.Offset
								rp.Status = ErrNone
								rp.Result = ErrNoneString
								rp.Match = true
								count++
								break
							}
						}
					}
				}
			case to := <-timer:
				status = ErrTimedOut
				result = to
				processErr = true
				break ConsumeLoop
			}
		}
		for i := 0; i < len(remoteOffsetMap); i++ {
			stopChan <- true
		}
		if processErr {
			for _, rp := range D.ReplicatedPartitions {
				if !keyFoundMap[rp.SetKey] {
					rp.Match = false
					rp.Result = result
					rp.Status = status
					currentDTNano := time.Now().UnixNano()
					currentLatencyNano := currentDTNano - rp.LocalDateTimeNano
					rp.LatencyNano = rp.LatencyNano + currentLatencyNano
					rp.LatencyMS = float32(rp.LatencyNano) / 1000000
				}
			}
		} else {
			result = ErrNoneString
			status = ErrNone
		}
		D.Summary.State = result
		D.Summary.Status = status
	}
	remoteClient.Close()
	wg.Done()
}

func getMaps(D *Detail) (topic string, keyValMap map[string]string, remoteOffsetMap map[int32]int64) {
	keyValMap = make(map[string]string, len(D.ReplicatedPartitions))
	for _, rp := range D.ReplicatedPartitions {
		if rp.Status != 0 {
			keyValMap[rp.SetKey] = rp.SetValue
		}
		for part := range rp.RemoteMetadata.PartitionOffsets {
			if remoteOffsetMap == nil {
				topic = rp.Topic
				remoteOffsetMap = rp.RemoteMetadata.PartitionOffsets
			} else {
				if rp.RemoteMetadata.PartitionOffsets[part] < remoteOffsetMap[part] {
					remoteOffsetMap[part] = rp.RemoteMetadata.PartitionOffsets[part]
				}
			}
		}
	}
	return
}

func needPRECheck(remoteMeta []kafkactl.TopicMeta) bool {
	for _, tm := range remoteMeta {
		if tm.Leader != tm.Replicas[0] {
			return true
		}
	}
	return false
}

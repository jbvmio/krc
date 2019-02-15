package db

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/jbvmio/krc"
	"github.com/jbvmio/krc/config"
	"github.com/jbvmio/krc/output"

	"github.com/jbvmio/jbkv"
)

var (
	MemoryTopicList []string
	MemoryStore     map[string]string
	mutex           = &sync.Mutex{}
)

func SaveTopicList(config *config.Config, topicList []string) {
	mutex.Lock()
	db, err := jbkv.NewBDB(config.DataDir)
	if err != nil {
		output.Failf("Error Opening DB Path %v: %v", config.DataDir, err)
	}
	k := []byte(krc.TopicListKey)
	v, _ := json.Marshal(topicList)
	err = db.SaveKV(k, v)
	if err != nil {
		output.Failf("Error Saving topic list to DB: %v", err)
	}
	db.Close()
	mutex.Unlock()
}

func GetTopicList(config *config.Config) []string {
	mutex.Lock()
	db, err := jbkv.NewBDB(config.DataDir)
	if err != nil {
		output.Failf("Error Opening DB Path %v: %v", config.DataDir, err)
	}
	k := []byte(krc.TopicListKey)
	v := db.GetValue(k)
	db.Close()
	var topics []string
	json.Unmarshal(v, &topics)
	mutex.Unlock()
	return topics
}

func SaveTopics(config *config.Config, details ...*krc.Detail) {
	mutex.Lock()
	db, err := jbkv.NewBDB(config.DataDir)
	if err != nil {
		output.Failf("Error Opening DB Path %v: %v", config.DataDir, err)
	}
	var wg sync.WaitGroup
	for _, detail := range details {
		j, _ := json.Marshal(detail)
		jsonString := fmt.Sprintf("%s", j)
		tc := krc.TopicCheck{
			Topic:   detail.Summary.Topic,
			Details: jsonString,
		}
		wg.Add(1)
		go saveDetail(db, &wg, tc)
	}
	wg.Wait()
	db.Close()
	mutex.Unlock()
}

func saveDetail(db *jbkv.BDB, wg *sync.WaitGroup, tc krc.TopicCheck) {
	k := []byte(tc.Topic)
	v := []byte(tc.Details)
	err := db.SaveKV(k, v)
	if err != nil {
		output.Failf("Error Saving details to DB: %v", err)
	}
	wg.Done()
}

func GetTopics(config *config.Config, topics ...string) []krc.TopicCheck {
	mutex.Lock()
	db, err := jbkv.NewBDB(config.DataDir)
	if err != nil {
		output.Failf("Error Opening DB Path %v: %v", config.DataDir, err)
	}
	var topicChecks []krc.TopicCheck
	for _, topic := range topics {
		k := []byte(topic)
		v := db.GetValue(k)
		tc := krc.TopicCheck{
			Topic:   topic,
			Details: fmt.Sprintf("%s", v),
		}
		topicChecks = append(topicChecks, tc)
	}
	db.Close()
	mutex.Unlock()
	return topicChecks
}

func RefreshMemStore(config *config.Config) {
	refreshStore := make(map[string]string)
	refresh := GetTopics(config, MemoryTopicList...)
	for _, t := range refresh {
		refreshStore[t.Topic] = t.Details
	}
	MemoryStore = refreshStore
}

func RemoveFromTopicList(config *config.Config, oldTopics ...string) {
	tmpMap := make(map[string]bool)
	var tmpList []string
	for _, ot := range oldTopics {
		tmpMap[ot] = true
	}
	for _, topic := range MemoryTopicList {
		if !tmpMap[topic] {
			tmpList = append(tmpList, topic)
		}
	}
	MemoryTopicList = tmpList
	SaveTopicList(config, MemoryTopicList)
}

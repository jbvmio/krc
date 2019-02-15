package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jbvmio/krc"
	"github.com/jbvmio/krc/check"
	"github.com/jbvmio/krc/config"
	"github.com/jbvmio/krc/db"
	"github.com/jbvmio/krc/output"
	krcSync "github.com/jbvmio/krc/sync"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/tidwall/pretty"
)

var (
	srv  *http.Server
	conf *config.Config
)

func InitAPI(config *config.Config) {
	conf = config
	output.Infof("Starting API ... ")
	r := mux.NewRouter()
	r.HandleFunc("/krc", rootHandler)
	r.HandleFunc("/krc/health", healthHandler)
	r.HandleFunc("/krc/stop", stopHandler)
	r.HandleFunc("/krc/status", statusHandler)
	r.HandleFunc("/krc/checks", checksHandler)
	r.HandleFunc("/krc/runcheck", runCheckHandler)
	r.HandleFunc("/krc/update", updateHandler)
	r.HandleFunc("/krc/topics", topicsHandler)
	r.HandleFunc("/krc/topics/{topic}", topicHandler)
	r.HandleFunc("/xkrc", rootHandler)
	r.HandleFunc("/xkrc/health", healthHandler)
	r.HandleFunc("/xkrc/restartcg", restartCGHandler)
	r.HandleFunc("/xkrc/resetparts", xResetPartsHandler)
	r.HandleFunc("/xkrc/status", xStatusHandler)
	r.HandleFunc("/xkrc/topics", xTopicsHandler)
	r.HandleFunc("/xkrc/topics/{topic}", xTopicHandler)
	loggingRouter := handlers.LoggingHandler(os.Stdout, r)
	srv = &http.Server{
		Addr:    config.APIEndpoint,
		Handler: loggingRouter,
	}
	output.Infof("API Waiting on Data Availability ...")
	for check.TotalChecksRun() < 1 {
		time.Sleep(time.Second * 2)
	}
	output.Infof("API Now Serving HTTP on %v", config.APIEndpoint)
	log.Fatal(srv.ListenAndServe())
}

func apiShutdown() {
	output.Infof("API Shutdown Request Received ... ")
	output.Infof("Shutting Down API ... ")
	for i := 0; i < krc.StopCount; i++ {
		krc.StopChannel <- true
	}
	time.Sleep(time.Second * krc.StopCount)
	if err := srv.Close(); err != nil {
		output.Failf("API Error: %v", err)
	}
	output.Infof("API Shutdown Complete.")
}

func apiRunCheck() {
	krc.ValidateDedicatedTopic(conf)
	check.RunCheck(conf)
}

func restartCG() {
	stopped := check.StopPeerCG()
	if stopped {
		output.Infof("CG Shutdown Complete.")
		check.StartPeerCG(conf)
	}
}

func restartCGHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"action": "restart consumer group"}`)
	go restartCG()
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
	vInfo := krc.VersionInfo{
		Name:       krc.KRCName,
		Version:    krc.BuildTime,
		Commit:     krc.CommitHash,
		Maintainer: krc.KRCMaintainer,
	}
	j, _ := json.Marshal(vInfo)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", pretty.Pretty(j))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"alive": true}`)
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"action": "shutdown"}`)
	go apiShutdown()
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"action": "sync topics"}`)
	go krcSync.TopicSyncCheck(conf)
}

func runCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"action": "replication check"}`)
	go apiRunCheck()
}

func topicsHandler(w http.ResponseWriter, r *http.Request) {
	var details []krc.Detail
	for k := range db.MemoryStore {
		var detail krc.Detail
		json.Unmarshal([]byte(db.MemoryStore[k]), &detail)
		details = append(details, detail)
	}
	j, _ := json.Marshal(details)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", pretty.Pretty(j))
}

func topicHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topic := vars["topic"]
	v := db.MemoryStore[topic]
	var detail krc.Detail
	json.Unmarshal([]byte(v), &detail)
	j, _ := json.Marshal(detail)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", pretty.Pretty(j))
}

func checksHandler(w http.ResponseWriter, r *http.Request) {
	var rParts []*krc.ReplicatedPartition
	for k := range db.MemoryStore {
		var detail krc.Detail
		json.Unmarshal([]byte(db.MemoryStore[k]), &detail)
		rParts = append(rParts, detail.ReplicatedPartitions...)
	}
	krc.StripMeta(rParts)
	j, _ := json.Marshal(rParts)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", pretty.Pretty(j))
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	var statuses []krc.Status
	for k := range db.MemoryStore {
		var detail krc.Detail
		json.Unmarshal([]byte(db.MemoryStore[k]), &detail)
		statuses = append(statuses, detail.Summary)
	}
	j, _ := json.Marshal(statuses)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", pretty.Pretty(j))
}

func xTopicsHandler(w http.ResponseWriter, r *http.Request) {
	j := check.GetXTopics("alltopics")
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", j)
}

func xTopicHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topic := vars["topic"]
	j := check.GetXTopics(topic)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", j)
}

func xStatusHandler(w http.ResponseWriter, r *http.Request) {
	j := check.GetXTopics("statuscheck")
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", j)
}

func xResetPartsHandler(w http.ResponseWriter, r *http.Request) {
	j := check.GetXTopics("resetpartitions")
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", j)
}

package krc

import "github.com/jbvmio/kafkactl"

const (
	ErrorStatusKey = "ERR_TOPICS_7777"
	TopicListKey   = "TOPIC_LIST"
	LocalKind      = "localDC"
	RemoteKind     = "crossDC"
	StopCount      = 5
)

var StopChannel chan bool

type ErrorStatus struct {
	ErrorStatusKey string           `json:"errStatusKey"` //key
	ErrorState     map[string]int16 `json:"errorState"`   //value
}

type TopicList struct {
	TopicListKey string   `json:"topicListKey"` //key
	TopicList    []string `json:"topicList"`    //value
}

// Currently Not Using
type PeerCheck struct {
	Topic   string `json:"topic"`   //key
	Sources string `json:"sources"` //value
}

type TopicCheck struct {
	Topic   string `json:"topic"`   //key
	Details string `json:"details"` //value
}

type Detail struct {
	Summary              Status                 `json:"summary"`
	ReplicatedPartitions []*ReplicatedPartition `json:"replicatedPartitions"`
}

type Status struct {
	Status int16  `json:"status"`
	Topic  string `json:"topic"`
	State  string `json:"state"`
}

type ReplicatedPartition struct {
	Kind                    string                  `json:"kind"`
	DateTimeSecs            int64                   `json:"dateTimeSecs"`
	LastSuccessDateTimeSecs int64                   `json:"lastSuccessDateTimeSecs"`
	SetKey                  string                  `json:"setKey"`
	SetValue                string                  `json:"setValue"`
	GetValue                string                  `json:"getValue"`
	Result                  string                  `json:"result"`
	Match                   bool                    `json:"match"`
	Status                  int16                   `json:"status"`
	Topic                   string                  `json:"topic"`
	LocalBroker             string                  `json:"localBroker"`
	LocalPartition          int32                   `json:"localPartition"`
	LocalOffset             int64                   `json:"localOffset"`
	LocalDateTimeNano       int64                   `json:"localDateTimeNano"`
	RemoteBroker            string                  `json:"remoteBroker"`
	RemotePartition         int32                   `json:"remotePartition"`
	RemoteOffset            int64                   `json:"remoteOffset"`
	RemoteDateTimeNano      int64                   `json:"remoteDateTimeNano"`
	PreviousResult          string                  `json:"previousResult"`
	PreviousStatus          int16                   `json:"previousStatus"`
	Metadata                kafkactl.TopicMeta      `json:"metadata,omitempty"`
	RemoteMetadata          kafkactl.TopicOffsetMap `json:"remoteMetadata,omitempty"`
	LatencyNano             int64                   `json:"latencyNano"`
	LatencyMS               float32                 `json:"latencyMS"`
	LocalNeedPRE            uint8                   `json:"localNeedPRE"`
	RemoteNeedPRE           uint8                   `json:"remoteNeedPRE"`
}

type PeerData struct {
	Topic   string   `json:"topic"`
	Sources []Source `json:"sources"`
}

type Source struct {
	Kind           string `json:"kind"`
	DateTimeSecs   int64  `json:"dateTimeSecs"`
	SetKey         string `json:"setKey"`
	Topic          string `json:"topic"`
	LocalBroker    string `json:"localBroker"`
	LocalPartition int32  `json:"localPartition"`
}

type PeerStatus struct {
	Kind               string `json:"kind"`
	Topic              string `json:"topic"`
	LocalBroker        string `json:"localBroker"`
	Partitions         int    `json:"partitions"`
	State              string `json:"state"`
	Status             int16  `json:"status"`
	SyncDifferenceSecs int64  `json:"syncDifferenceSecs"`
	LastSeenSecsAgo    int64  `json:"lastSeenSecsAgo"`
}

// ErrorState Descriptions
const (
	NeedLocalPRE  uint8 = 1
	NeedRemotePRE uint8 = 1

	ErrInitializing       int16  = 1
	ErrInitializingString string = "Initializing"

	ErrNone       int16  = 2
	ErrNoneString string = "Success"

	ErrTopicMeta       int16  = 3
	ErrTopicMetaString string = "ErrTopicMetadata"

	ErrSendMsg       int16  = 4
	ErrSendMsgString string = "ErrSendMsg"

	ErrTopicOrPartNotFound       int16  = 5
	ErrTopicOrPartNotFoundString string = "ErrTopicOrPartNotFound"

	ErrTimedOut       int16  = 6
	ErrTimedOutString string = "ErrTimedOut"

	ErrPartitionOrOffset       int16  = 7777
	ErrPartitionOrOffsetString string = "ErrPartitionOrOffset"

	ErrCreateClient       int16  = -1001
	ErrCreateClientString string = "ErrCreateClient"
)

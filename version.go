package krc

import "fmt"

const (
	KRCName       string = "Kafka Replication Check (krc)"
	KRCMaintainer string = "JBVMIO"
)

var (
	BuildTime  string
	CommitHash string
)

type VersionInfo struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	Maintainer string `json:"maintainer"`
}

func PrintVersion() {
	fmt.Printf("%s\n", KRCName)
	fmt.Printf("%s\n", KRCMaintainer)
	fmt.Printf("Version   : %s\n", BuildTime)
	fmt.Printf("Commit    : %s\n", CommitHash)
}

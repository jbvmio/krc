package ops

import (
	"crypto/sha1"
	"os"
)

func FileExists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}

func GetSHA1(data []byte) []byte {
	val := make([]byte, 0, 20)
	sha := sha1.Sum(data)
	val = append(val, sha[:]...)
	return val
}

func FilterUnique(strSlice []string) []string {
	keys := make(map[string]bool)
	var list []string
	for _, entry := range strSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

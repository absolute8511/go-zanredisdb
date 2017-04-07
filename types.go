package zanredisdb

import (
	"strings"
	"time"
)

var (
	FailedOnClusterChanged = "ERR_CLUSTER_CHANGED"
	FailedOnNotLeader      = "E_FAILED_ON_NOT_LEADER"
	FailedOnNotWritable    = "E_FAILED_ON_NOT_WRITABLE"
)

func IsFailedOnClusterChanged(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), FailedOnClusterChanged)
	}
	return false
}

func IsFailedOnNotWritable(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), FailedOnNotWritable)
	}
	return false
}

type Conf struct {
	LookupList   []string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	TendInterval int64
	Namespace    string
	Password     string
}

// api data response type
type node struct {
	BroadcastAddress string `json:"broadcast_address"`
	Hostname         string `json:"hostname"`
	RedisPort        string `json:"redis_port"`
	HTTPPort         string `json:"http_port"`
	Version          string `json:"version"`
}

type PartitionNodeInfo struct {
	Leader   node   `json:"leader"`
	Replicas []node `json:"replicas"`
}

type queryNamespaceResp struct {
	Epoch        int64                     `json:"epoch"`
	EngType      string                    `json:"eng_type"`
	Partitions   map[int]PartitionNodeInfo `json:"partitions"`
	PartitionNum int                       `json:"partition_num"`
}

type NodeInfo struct {
	RegID             uint64
	ID                string
	NodeIP            string
	Hostname          string
	RedisPort         string
	HttpPort          string
	RpcPort           string
	RaftTransportAddr string
	Version           string
	Tags              map[string]bool
	DataRoot          string
	RsyncModule       string
	Epoch             int64
}

type listPDResp struct {
	PDNodes  []NodeInfo `json:"pdnodes"`
	PDLeader NodeInfo   `json:"pdleader"`
}

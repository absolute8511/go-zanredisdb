package zanredisdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	MIN_RETRY_SLEEP = time.Millisecond * 32
)

type ZanRedisClient struct {
	conf    *Conf
	cluster *Cluster
}

func NewZanRedisClient(conf *Conf) *ZanRedisClient {
	return &ZanRedisClient{
		conf: conf,
	}
}

func (self *ZanRedisClient) Start() {
	self.cluster = NewCluster(self.conf)
}

func (self *ZanRedisClient) Stop() {
	if self.cluster != nil {
		self.cluster.Close()
	}
}

func DoRedisCmd(conn redis.Conn, cmdName string, args ...interface{}) (reply interface{}, err error) {
	defer conn.Close()
	rsp, err := conn.Do(cmdName, args...)
	return rsp, err
}

func (self *ZanRedisClient) DoRedis(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) (interface{}, error) {
	retry := uint32(0)
	var err error
	var rsp interface{}
	var conn redis.Conn
	var sleeped time.Duration
	ro := self.conf.ReadTimeout / 2
	if ro == 0 {
		ro = time.Second
	}
	for retry < 3 || sleeped < ro+time.Millisecond*100 {
		retry++
		conn, err = self.cluster.GetConn(shardingKey, toLeader)
		if err != nil {
			self.cluster.MaybeTriggerCheckForError(err, 0)
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
			sleeped += MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry))
			continue
		}
		rsp, err = DoRedisCmd(conn, cmd, args...)

		if err != nil {
			clusterChanged := self.cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Infof("command err for cluster changed: %v, %v", shardingKey, args)
				// we can retry for cluster error
			} else if _, ok := err.(redis.Error); ok {
				// other error from command reply no need retry
				// can fail fast for some un-recovery error
				break
			}
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
			sleeped += MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry))
		} else {
			break
		}
	}
	return rsp, err
}

func (self *ZanRedisClient) KVGet(set string, key []byte) ([]byte, error) {
	pk := NewPKey(self.conf.Namespace, set, key)
	return redis.Bytes(self.DoRedis("GET", pk.ShardingKey(), true, pk.RawKey))
}

func (self *ZanRedisClient) KVSet(set string, key []byte, value []byte) error {
	pk := NewPKey(self.conf.Namespace, set, key)
	_, err := redis.String(self.DoRedis("SET", pk.ShardingKey(), true, pk.RawKey, value))
	return err
}

func (self *ZanRedisClient) KVDel(set string, key []byte, value []byte) error {
	pk := NewPKey(self.conf.Namespace, set, key)
	_, err := redis.Int(self.DoRedis("DEL", pk.ShardingKey(), true, pk.RawKey))
	return err
}

func (self *ZanRedisClient) KVMGet(pKeys ...*PKey) ([][]byte, error) {
	for _, pk := range pKeys {
		if pk.Namespace != self.conf.Namespace {
			return nil, fmt.Errorf("invalid Namespace:%s", pk.Namespace)
		}
	}

	partNum := self.cluster.GetPartitionNum()
	keysPart := make([]int, len(pKeys))

	type packedKeys struct {
		shardingKey []byte
		rawKeys     []interface{}
	}

	partitionKeys := make([]packedKeys, partNum)

	for i, pk := range pKeys {
		partID := GetHashedPartitionID(pk.ShardingKey(), partNum)
		keysPart[i] = partID

		partitionKeys[partID].rawKeys = append(partitionKeys[partID].rawKeys, pk.RawKey)
		partitionKeys[partID].shardingKey = pk.ShardingKey()
	}

	partitionValues := make([][][]byte, partNum)

	var lock sync.Mutex
	wg := sync.WaitGroup{}
	for partID, keys := range partitionKeys {
		wg.Add(1)
		go func(keys packedKeys, partID int) {
			defer wg.Done()
			vals, _ := redis.ByteSlices(self.DoRedis("MGET", keys.shardingKey,
				true, keys.rawKeys...))
			lock.Lock()
			partitionValues[partID] = vals
			lock.Unlock()

		}(keys, partID)
	}
	wg.Wait()

	resultVals := make([][]byte, len(pKeys))
	partPos := make([]int, partNum)

	for i, partID := range keysPart {
		vals := partitionValues[partID]
		pos := partPos[partID]

		if vals == nil || pos >= len(vals) {
			resultVals[i] = nil
		} else {
			resultVals[i] = vals[pos]
		}

		partPos[partID] = pos + 1
	}

	return resultVals, nil
}

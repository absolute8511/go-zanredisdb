package zanredisdb

import (
	"github.com/garyburd/redigo/redis"
	"time"
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
		rsp, err = conn.Do(cmd, args...)
		conn.Close()
		if err != nil {
			self.cluster.MaybeTriggerCheckForError(err, 0)
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

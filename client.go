package zanredisdb

import (
	"github.com/garyburd/redigo/redis"
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
	conn, err := self.cluster.GetConn(shardingKey, toLeader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.Do(cmd, args...)
}

func (self *ZanRedisClient) KVGet(set string, key []byte) ([]byte, error) {
	pk := NewPKey(self.conf.Namespace, set, key)
	conn, err := self.cluster.GetConn(pk.ShardingKey(), true)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return redis.Bytes(conn.Do("GET", pk.RawKey))
}

func (self *ZanRedisClient) KVSet(set string, key []byte, value []byte) error {
	pk := NewPKey(self.conf.Namespace, set, key)
	conn, err := self.cluster.GetConn(pk.ShardingKey(), true)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = redis.String(conn.Do("SET", pk.RawKey, value))
	return err
}

func (self *ZanRedisClient) KVDel(set string, key []byte, value []byte) error {
	pk := NewPKey(self.conf.Namespace, set, key)
	conn, err := self.cluster.GetConn(pk.ShardingKey(), true)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = redis.Int(conn.Do("DEL", pk.RawKey))
	return err
}

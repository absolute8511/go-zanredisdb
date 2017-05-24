package zanredisdb

import (
	"errors"
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
			clusterChanged := self.cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Detailf("command err for cluster changed: %v, %v", shardingKey, args)
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

func (client *ZanRedisClient) DoScan(cmd string, ns string, args ...interface{}) ([]interface{}, error) {
	retry := uint32(0)
	var err error
	var rsps []interface{}
	var conns []redis.Conn
	var sleeped time.Duration
	var wg sync.WaitGroup
	ro := client.conf.ReadTimeout / 2
	if ro == 0 {
		ro = time.Second
	}
	for retry < 3 || sleeped < ro+time.Millisecond*100 {
		retry++
		conns, err = client.cluster.GetConns(ns)
		if err != nil {
			client.cluster.MaybeTriggerCheckForError(err, 0)
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
			sleeped += MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry))
			continue
		}
		rsps = make([]interface{}, len(conns))
		for i, c := range conns {
			wg.Add(1)
			go func(index int, c redis.Conn) {
				rsps[index], err = c.Do(cmd, args...)
				wg.Add(-1)
			}(i, c)
		}
		wg.Wait()
		if err != nil {
			clusterChanged := client.cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Detailf("command err for cluster changed: %v, %v, %v", cmd, ns, args)
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
	return rsps, err
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

func (client *ZanRedisClient) scanChannel(cmd, set string, ch chan []byte) {
	cursor := []byte("")
	for {
		sk := NewScanKey(client.conf.Namespace, set, cursor)
		result, err := redis.Values(client.DoRedis(cmd, sk.ShardingKey(), true, sk.RawKey))
		if err != nil {
			close(ch)
			break
		}
		if len(result) != 2 {
			close(ch)
			break
		}

		cursor = result[0].([]byte)
		for _, res := range result[1].([]interface{}) {
			ch <- res.([]byte)
		}

		if len(cursor) == 0 {
			close(ch)
			break
		}

	}
}

func (client *ZanRedisClient) scan(cmd, set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	sk := NewPKey(client.conf.Namespace, set, cursor)
	result, err := redis.Values(client.DoRedis(cmd, sk.ShardingKey(), true, sk.RawKey))
	if err != nil {
		return nil, nil, err
	}

	if len(result) != 2 {
		return nil, nil, errors.New("no enough return parameters")
	}

	return result[0].([]byte), result[1].([][]byte), nil
}

func (client *ZanRedisClient) KVScanChannel(set string, ch chan []byte) {
	client.scanChannel("SCAN", set, ch)
}

func (client *ZanRedisClient) KVScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.scan("SCAN", set, count, cursor)
}

func (client *ZanRedisClient) HScanChannel(set string, ch chan []byte) {
	client.scanChannel("HSCAN", set, ch)
}

func (client *ZanRedisClient) HScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.scan("HSCAN", set, count, cursor)
}

func (client *ZanRedisClient) SScanChannel(set string, ch chan []byte) {
	client.scanChannel("SSCAN", set, ch)
}

func (client *ZanRedisClient) SScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.scan("SSCAN", set, count, cursor)
}

func (client *ZanRedisClient) ZScanChannel(set string, ch chan []byte) {
	client.scanChannel("ZSCAN", set, ch)
}

func (client *ZanRedisClient) ZScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.scan("ZSCAN", set, count, cursor)
}

func (client *ZanRedisClient) LScanChannel(set string, ch chan []byte) {
	client.scanChannel("LSCAN", set, ch)
}

func (client *ZanRedisClient) LScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.scan("LSCAN", set, count, cursor)
}

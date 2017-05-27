package zanredisdb

import (
	"bytes"
	"encoding/base64"
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

func (client *ZanRedisClient) DoScan(cmd, set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	retry := uint32(0)
	var err error
	var rsps []interface{}
	var conns []redis.Conn
	var sleeped time.Duration
	var wg sync.WaitGroup
	ro := client.conf.ReadTimeout / 2
	connCursorMap := make(map[string]string)
	var lock sync.Mutex
	var hosts []string
	if ro == 0 {
		ro = time.Second
	}
	ns := client.conf.Namespace
	for retry < 3 || sleeped < ro+time.Millisecond*100 {
		retry++
		if len(cursor) == 0 {
			conns, hosts, err = client.cluster.GetConns()
		} else {
			decodedCursor, err := base64.StdEncoding.DecodeString(string(cursor))
			if err != nil {
				return nil, nil, err
			}

			cursors := bytes.Split(decodedCursor, []byte("|"))
			if len(cursors) <= 0 {
				return nil, nil, errors.New("invalid cursor")
			}

			for _, c := range cursors {
				if len(c) == 0 {
					continue
				}
				splits := bytes.Split(c, []byte("@"))
				if len(splits) != 2 {
					return nil, nil, errors.New("invalid cursor")
				}
				hosts = append(hosts, string(splits[0]))
				connCursorMap[string(splits[0])] = string(splits[1])
			}
			conns, hosts, err = client.cluster.GetConnsByHosts(hosts)
		}
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
				sk := NewScanKey(ns, set, count, cursor)
				ay, err := redis.Values(c.Do(cmd, sk.RawKey))
				if err == nil &&
					len(ay) == 2 {
					originCursor := ay[0].([]byte)
					if len(originCursor) != 0 {
						var cursor []byte
						cursor = append(cursor, []byte(hosts[index])...)
						cursor = append(cursor, []byte("@")...)
						cursor = append(cursor, originCursor...)
						lock.Lock()
						connCursorMap[hosts[index]] = string(cursor)
						lock.Unlock()

						ay[0] = cursor
					}
					rsps[index] = ay
				}
				wg.Add(-1)
			}(i, c)
		}
		wg.Wait()
		if err != nil {
			clusterChanged := client.cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Detailf("command err for cluster changed: %v, %v", cmd, ns)
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
	var newCursor []byte
	var keys [][]byte
	first := true
	for _, rsp := range rsps {
		if rsp == nil {
			continue
		}
		if len(rsp.([]interface{})) != 2 {
			continue
		}
		r := rsp.([]interface{})
		c := r[0].([]byte)
		if len(c) != 0 {
			if !first {
				newCursor = append(newCursor, '|')
			} else {
				first = false
			}
			newCursor = append(newCursor, c...)
		}
		for _, k := range r[1].([]interface{}) {
			newKey := k.([]byte)
			keys = append(keys, newKey)
		}
	}
	encodedCursor := base64.StdEncoding.EncodeToString(newCursor)
	return []byte(encodedCursor), keys, err
}

func (client *ZanRedisClient) DoScanChannel(cmd, set string, ch chan []byte) {
	retry := uint32(0)
	var err error
	var conns []redis.Conn
	var wg sync.WaitGroup
	var sleeped time.Duration
	ro := client.conf.ReadTimeout / 2
	if ro == 0 {
		ro = time.Second
	}
	ns := client.conf.Namespace
	for retry < 3 || sleeped < ro+time.Millisecond*100 {
		retry++
		conns, _, err = client.cluster.GetConns()
		if err != nil {
			client.cluster.MaybeTriggerCheckForError(err, 0)
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
			sleeped += MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry))
			continue
		}
		for _, c := range conns {
			wg.Add(1)
			go func(c redis.Conn) {
				cursor := []byte("")
				for {
					sk := NewScanKey(ns, set, 10, cursor)
					ay, err := redis.Values(c.Do(cmd, sk.RawKey))
					if err != nil {
						break
					}
					if len(ay) != 2 {
						break
					}

					cursor = ay[0].([]byte)
					for _, res := range ay[1].([]interface{}) {
						val := res.([]byte)
						ch <- val
					}

					if len(cursor) == 0 {
						break
					}
				}
				wg.Add(-1)
			}(c)
		}
		wg.Wait()
		if err != nil {
			clusterChanged := client.cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Detailf("command err for cluster changed: %v, %v", cmd, ns)
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
	close(ch)
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

func (client *ZanRedisClient) KVScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("SCAN", set, ch)
}

func (client *ZanRedisClient) KVScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("SCAN", set, count, cursor)
}

func (client *ZanRedisClient) HScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("HSCAN", set, ch)
}

func (client *ZanRedisClient) HScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("HSCAN", set, count, cursor)
}

func (client *ZanRedisClient) SScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("SSCAN", set, ch)
}

func (client *ZanRedisClient) SScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("SSCAN", set, count, cursor)
}

func (client *ZanRedisClient) ZScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("ZSCAN", set, ch)
}

func (client *ZanRedisClient) ZScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("ZSCAN", set, count, cursor)
}

func (client *ZanRedisClient) LScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("LSCAN", set, ch)
}

func (client *ZanRedisClient) LScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("LSCAN", set, count, cursor)
}

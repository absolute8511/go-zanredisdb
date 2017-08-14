package zanredisdb

import (
	"bytes"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/siddontang/goredis"
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

func (client *ZanRedisClient) DoScan(cmd, tp, set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	retry := uint32(0)
	var err error
	var rsps []interface{}
	var conns []redis.Conn
	var sleeped time.Duration
	var wg sync.WaitGroup
	ro := client.conf.ReadTimeout / 2
	connCursorMap := make(map[string]string)
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
				cur := connCursorMap[hosts[index]]
				var tmp bytes.Buffer
				tmp.WriteString(ns)
				tmp.WriteString(":")
				tmp.WriteString(set)
				tmp.WriteString(":")
				tmp.WriteString(cur)
				ay, err := redis.Values(c.Do(cmd, tmp.Bytes(), tp, "count", count))
				if err == nil &&
					len(ay) == 2 {
					originCursor := ay[0].([]byte)
					if len(originCursor) != 0 {
						var cursor []byte
						cursor = append(cursor, []byte(hosts[index])...)
						cursor = append(cursor, []byte("@")...)
						cursor = append(cursor, originCursor...)
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
				levelLog.Detailf("advscan command err for cluster changed: %v, %v", tp, ns)
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

func (client *ZanRedisClient) DoScanChannel(cmd, tp, set string, ch chan []byte) {
	defer func() {
		if err := recover(); err != nil {
			levelLog.Errorf("get error. [err=%v]\n", err)
		}
	}()
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
				defer func() {
					if err := recover(); err != nil {
						levelLog.Errorf("get error. [err=%v]\n", err)
					}
				}()
				cursor := []byte("")

				var tmp bytes.Buffer
				for {
					tmp.Truncate(0)
					tmp.WriteString(ns)
					tmp.WriteString(":")
					tmp.WriteString(set)
					tmp.WriteString(":")
					tmp.Write(cursor)
					ay, err := redis.Values(c.Do(cmd, tmp.Bytes(), tp, "count", 100))
					if err != nil {
						levelLog.Errorf("get error when DoScanChannel. [err=%v]\n", err)
						break
					}
					if len(ay) != 2 {
						levelLog.Errorf("response length is not 2 when DoScanChannel. [len=%d]\n", len(ay))
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
				levelLog.Detailf("advscan command err for cluster changed: %v, %v", tp, ns)
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

func (client *ZanRedisClient) DoFullScanChannel(tp, set string, ch chan interface{}) {
	defer func() {
		if err := recover(); err != nil {
			levelLog.Errorf("get error. [err=%v]\n", err)
		}
	}()
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
				defer func() {
					if err := recover(); err != nil {
						levelLog.Errorf("get error. [err=%v]\n", err)
					}
				}()
				cursor := []byte("")

				var tmp bytes.Buffer
				for {
					tmp.Truncate(0)
					tmp.WriteString(ns)
					tmp.WriteString(":")
					tmp.WriteString(set)
					tmp.WriteString(":")
					tmp.Write(cursor)
					ay, err := redis.Values(c.Do("FULLSCAN", tmp.Bytes(), tp, "count", 100))
					if err != nil {
						levelLog.Errorf("get error when DoFullScan. [err=%v]\n", err)
						break
					}
					if len(ay) != 2 {
						levelLog.Errorf("response length is not 2 when DoFullScan. [len=%d]\n", len(ay))
						break
					}

					cursor = ay[0].([]byte)
					a, err := goredis.MultiBulk(ay[1], nil)
					if err != nil {
						levelLog.Errorf("get error. [Err=%v]\n", err)
						break
					}

					ch <- a

					if len(cursor) == 0 {
						break
					}
				}
				wg.Done()
			}(c)
		}
		wg.Wait()
		if err != nil {
			clusterChanged := client.cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Detailf("fullscan command err for cluster changed: %v, %v", tp, ns)
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

func (client *ZanRedisClient) DoFullScan(cmd, tp, set string, count int, cursor []byte) ([]byte, []interface{}, error) {
	retry := uint32(0)
	var err error
	var rsps []interface{}
	var conns []redis.Conn
	var sleeped time.Duration
	var wg sync.WaitGroup
	ro := client.conf.ReadTimeout / 2
	connCursorMap := make(map[string]string)
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
				cur := connCursorMap[hosts[index]]
				var tmp bytes.Buffer
				tmp.WriteString(ns)
				tmp.WriteString(":")
				tmp.WriteString(set)
				tmp.WriteString(":")
				tmp.WriteString(cur)
				ay, err := redis.Values(c.Do(cmd, tmp.Bytes(), tp, "count", count))
				if err == nil &&
					len(ay) == 2 {
					originCursor := ay[0].([]byte)
					if len(originCursor) != 0 {
						var cursor []byte
						cursor = append(cursor, []byte(hosts[index])...)
						cursor = append(cursor, []byte("@")...)
						cursor = append(cursor, originCursor...)
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
				levelLog.Detailf("advscan command err for cluster changed: %v, %v", tp, ns)
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
	var result []interface{}

	for _, rsp := range rsps {
		if rsp == nil || len(rsp.([]interface{})) != 2 {
			continue
		}

		r := rsp.([]interface{})
		c := r[0].([]byte)

		if len(newCursor) > 0 {
			newCursor = append(newCursor, '|')
		}
		newCursor = append(newCursor, c...)
		result = append(result, r[1].([]interface{})...)
	}

	encodedCursor := make([]byte, base64.StdEncoding.EncodedLen(len(newCursor)))
	base64.StdEncoding.Encode(encodedCursor, newCursor)

	return encodedCursor, result, err
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

func (self *ZanRedisClient) KVSetnx(set string, key []byte, value []byte) (int, error) {
	pk := NewPKey(self.conf.Namespace, set, key)
	return redis.Int(self.DoRedis("setnx", pk.ShardingKey(), true, pk.RawKey, value))
}

func (client *ZanRedisClient) LLen(set string, key []byte) (int, error) {
	pk := NewPKey(client.conf.Namespace, set, key)
	return redis.Int(client.DoRedis("LLEN", pk.ShardingKey(), true, pk.RawKey))
}

func (client *ZanRedisClient) LRange(set string, key []byte, start, top int) ([]interface{}, error) {
	pk := NewPKey(client.conf.Namespace, set, key)
	return redis.MultiBulk(client.DoRedis("LRANGE", pk.ShardingKey(), true, pk.RawKey, start, top))
}

func (client *ZanRedisClient) ZCard(set string, key []byte) (int, error) {
	pk := NewPKey(client.conf.Namespace, set, key)
	return redis.Int(client.DoRedis("ZCARD", pk.ShardingKey(), true, pk.RawKey))
}

func (client *ZanRedisClient) ZRange(set string, key []byte, start, top int, withscores bool) ([]interface{}, error) {
	pk := NewPKey(client.conf.Namespace, set, key)
	if withscores {
		return redis.MultiBulk(client.DoRedis("ZRANGE", pk.ShardingKey(), true, pk.RawKey, start, top, "WITHSCORES"))
	} else {
		return redis.MultiBulk(client.DoRedis("ZRANGE", pk.ShardingKey(), true, pk.RawKey, start, top))
	}
}

func (client *ZanRedisClient) SMembers(set string, key []byte) ([]interface{}, error) {
	pk := NewPKey(client.conf.Namespace, set, key)
	return redis.MultiBulk(client.DoRedis("SMEMBERS", pk.ShardingKey(), true, pk.RawKey))
}

func (client *ZanRedisClient) HGetAll(set string, key []byte) ([]interface{}, error) {
	pk := NewPKey(client.conf.Namespace, set, key)
	return redis.MultiBulk(client.DoRedis("HGETALL", pk.ShardingKey(), true, pk.RawKey))
}

func (client *ZanRedisClient) FullScanChannel(tp, set string, ch chan interface{}) {
	client.DoFullScanChannel(tp, set, ch)
}

func (client *ZanRedisClient) FullScan(tp, set string, count int, cursor []byte) ([]byte, []interface{}, error) {
	return client.DoFullScan("FULLSCAN", tp, set, count, cursor)
}

func (client *ZanRedisClient) AdvScanChannel(tp, set string, ch chan []byte) {
	client.DoScanChannel("ADVSCAN", tp, set, ch)
}

func (client *ZanRedisClient) AdvScan(tp, set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("ADVSCAN", tp, set, count, cursor)
}

func (client *ZanRedisClient) KVScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("SCAN", "KV", set, ch)
}

func (client *ZanRedisClient) KVScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("SCAN", "KV", set, count, cursor)
}

func (client *ZanRedisClient) HScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("HSCAN", "HASH", set, ch)
}

func (client *ZanRedisClient) HScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("HSCAN", "HASH", set, count, cursor)
}

func (client *ZanRedisClient) SScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("SSCAN", "SET", set, ch)
}

func (client *ZanRedisClient) SScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("SSCAN", "SET", set, count, cursor)
}

func (client *ZanRedisClient) ZScanChannel(set string, ch chan []byte) {
	client.DoScanChannel("ZSCAN", "ZSET", set, ch)
}

func (client *ZanRedisClient) ZScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("ZSCAN", "ZSET", set, count, cursor)
}

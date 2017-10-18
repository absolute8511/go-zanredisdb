package zanredisdb

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redigo/redis"
)

const (
	MIN_RETRY_SLEEP = time.Millisecond * 16
)

type PipelineCmd struct {
	CmdName     string
	ShardingKey []byte
	ToLeader    bool
	Args        []interface{}
}
type PipelineCmdList []PipelineCmd

func (pl *PipelineCmdList) Add(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) {
	*pl = append(*pl, PipelineCmd{CmdName: cmd,
		ShardingKey: shardingKey,
		ToLeader:    toLeader,
		Args:        args})
}

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

// while deploy across two datacenters, to improve read latency we can
// enable this, and set toLeader=false while calling DoRedis
// if there is no node in the same data center, we will fallback to the random node in other dc.
func (self *ZanRedisClient) SwitchSameDC(useSameDC bool) {
	if useSameDC {
		atomic.StoreInt32(&self.cluster.choseSameDCFirst, 1)
	} else {
		atomic.StoreInt32(&self.cluster.choseSameDCFirst, 0)
	}
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

func (self *ZanRedisClient) doPipelineCmd(cmds PipelineCmdList,
	rsps []interface{}, errs []error) ([]interface{}, []error) {
	reqs := make([]redis.Conn, len(cmds))
	reusedConn := make(map[string]redis.Conn)
	redisHosts := make(map[string]*RedisHost)
	retryStart := time.Now()
	for i, cmd := range cmds {
		// avoid retry the success cmd while retrying the failed
		if rsps[i] != nil {
			continue
		}
		if errs[i] != nil {
			if _, ok := errs[i].(redis.Error); ok {
				continue
			}
		}
		errs[i] = nil
		node, err := self.cluster.GetNodeHost(cmd.ShardingKey, cmd.ToLeader)
		if err != nil {
			levelLog.Infof("command err while get conn: %v, %v", cmd, err)
			errs[i] = err
			continue
		}
		conn, ok := reusedConn[node.addr]
		if !ok {
			conn, err = node.connPool.Get(0)
			if err != nil {
				levelLog.Infof("command err while get conn: %v, %v", cmd, err)
				errs[i] = err
				continue
			}
			reusedConn[node.addr] = conn
			redisHosts[node.addr] = node
		}
		reqs[i] = conn
		cost := time.Since(retryStart)
		if cost > time.Millisecond*10 {
			levelLog.Infof("pipeline command get conn slow, cost: %v", cost)
		}
		if levelLog.Level() > 2 {
			levelLog.Debugf("command %v send to conn: %v", cmd, conn.RemoteAddrStr())
		}
		errs[i] = conn.Send(cmd.CmdName, cmd.Args...)
	}
	for addr, c := range reusedConn {
		if c != nil {
			err := c.Flush()
			if err != nil {
				for i, reqConn := range reqs {
					if reqConn != nil && reqConn.RemoteAddrStr() == addr {
						errs[i] = err
					}
				}
				redisHost, ok := redisHosts[addr]
				if ok {
					redisHost.MaybeIncFailed(err)
				}
			}
		}
	}

	for i, c := range reqs {
		if c != nil && errs[i] == nil {
			retryStart := time.Now()
			rsps[i], errs[i] = c.Receive()
			cost := time.Since(retryStart)
			if cost > time.Millisecond*100 {
				levelLog.Infof("pipeline command %v to node %v slow, cost: %v", cmds[i], c.RemoteAddrStr(), cost)
			}
			redisHost, ok := redisHosts[c.RemoteAddrStr()]
			if errs[i] != nil {
				if ok {
					redisHost.MaybeIncFailed(errs[i])
				}
			} else {
				if ok {
					redisHost.ResetFailed()
				}
			}
		}
	}
	for _, c := range reusedConn {
		if c != nil {
			c.Close()
		}
	}
	return rsps, errs
}

func (self *ZanRedisClient) FlushAndWaitPipelineCmd(cmds PipelineCmdList) ([]interface{}, []error) {
	retry := uint32(0)
	rsps := make([]interface{}, len(cmds))
	errs := make([]error, len(cmds))
	reqStart := time.Now()
	ro := self.conf.ReadTimeout
	if ro == 0 {
		ro = time.Second
	}
	ro = ro * time.Duration(len(cmds))
	for retry < 3 || time.Since(reqStart) < ro {
		retry++
		retryStart := time.Now()
		rsps, errs = self.doPipelineCmd(cmds, rsps, errs)
		cost := time.Since(retryStart)
		if cost > time.Millisecond*200 {
			levelLog.Infof("pipeline command %v slow, cost: %v", len(cmds), cost)
		}
		needRetry := false
		for i, err := range errs {
			if err != nil {
				clusterChanged := self.cluster.MaybeTriggerCheckForError(err, 0)
				if clusterChanged {
					levelLog.Infof("pipeline command err for cluster changed: %v, %v", cmds[i], err)
					needRetry = true
					break
				} else {
					levelLog.Infof("pipeline command err: %v, %v", cmds[i], err)
				}
			}
		}
		if needRetry {
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
		} else {
			break
		}
	}
	return rsps, errs
}

func (self *ZanRedisClient) DoRedis(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) (interface{}, error) {
	retry := uint32(0)
	var err error
	var rsp interface{}
	var conn redis.Conn
	var redisHost *RedisHost
	reqStart := time.Now()
	ro := self.conf.ReadTimeout
	if ro == 0 {
		ro = time.Second
	}
	for retry < 3 || time.Since(reqStart) < ro {
		retry++
		retryStart := time.Now()
		redisHost, conn, err = self.cluster.GetHostAndConn(shardingKey, toLeader)
		cost1 := time.Since(retryStart)
		if cost1 > time.Millisecond*100 {
			levelLog.Infof("command %v-%v slow to get conn, cost: %v", cmd, string(shardingKey), cost1)
		}
		if err != nil {
			clusterChanged := self.cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Infof("command err for cluster changed: %v", cmd)
			} else {
				levelLog.Infof("command err : %v, %v", cmd, err.Error())
			}
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
			continue
		}

		remote := conn.RemoteAddrStr()
		rsp, err = DoRedisCmd(conn, cmd, args...)
		cost := time.Since(retryStart)
		if cost > time.Millisecond*200 {
			levelLog.Debugf("command %v-%v to node %v slow, cost: %v, get conn cost %v", cmd, string(shardingKey),
				remote, cost, cost1)
		}

		if err != nil {
			clusterChanged := self.cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Infof("command err for cluster changed: %v, %v, node: %v", shardingKey, args, remote)
				// we can retry for cluster error
			} else if _, ok := err.(redis.Error); ok {
				// other error from command reply no need retry
				// can fail fast for some un-recovery error
				break
			}
			redisHost.MaybeIncFailed(err)
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
		} else {
			redisHost.ResetFailed()
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
	if partNum == 0 {
		return nil, errNoAnyPartitions
	}
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

	var pipelines PipelineCmdList
	for _, keys := range partitionKeys {
		pipelines.Add("MGET", keys.shardingKey, true, keys.rawKeys...)
	}
	rsps, errs := self.FlushAndWaitPipelineCmd(pipelines)
	for i, rsp := range rsps {
		vals, _ := redis.ByteSlices(rsp, errs[i])
		partitionValues[i] = vals
	}

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

func (self *ZanRedisClient) KVSetNX(set string, key []byte, value []byte) (int, error) {
	pk := NewPKey(self.conf.Namespace, set, key)
	return redis.Int(self.DoRedis("setnx", pk.ShardingKey(), true, pk.RawKey, value))
}

func (client *ZanRedisClient) FullScanChannel(tp, set string, stopC chan struct{}) chan interface{} {
	return client.DoFullScanChannel(tp, set, stopC)
}

func (client *ZanRedisClient) FullScan(tp, set string, count int, cursor []byte) ([]byte, []interface{}, error) {
	return client.DoFullScan("FULLSCAN", tp, set, count, cursor)
}

func (client *ZanRedisClient) AdvScanChannel(tp, set string, stopC chan struct{}) chan []byte {
	return client.DoScanChannel("ADVSCAN", tp, set, stopC)
}

func (client *ZanRedisClient) AdvScan(tp, set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("ADVSCAN", tp, set, count, cursor)
}

func (client *ZanRedisClient) KVScanChannel(set string, stopC chan struct{}) chan []byte {
	return client.DoScanChannel("SCAN", "KV", set, stopC)
}

func (client *ZanRedisClient) KVScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("SCAN", "KV", set, count, cursor)
}

func (client *ZanRedisClient) HScanChannel(set string, stopC chan struct{}) chan []byte {
	return client.DoScanChannel("HSCAN", "HASH", set, stopC)
}

func (client *ZanRedisClient) HScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("HSCAN", "HASH", set, count, cursor)
}

func (client *ZanRedisClient) SScanChannel(set string, stopC chan struct{}) chan []byte {
	return client.DoScanChannel("SSCAN", "SET", set, stopC)
}

func (client *ZanRedisClient) SScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("SSCAN", "SET", set, count, cursor)
}

func (client *ZanRedisClient) ZScanChannel(set string, stopC chan struct{}) chan []byte {
	return client.DoScanChannel("ZSCAN", "ZSET", set, stopC)
}

func (client *ZanRedisClient) ZScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("ZSCAN", "ZSET", set, count, cursor)
}

func (client *ZanRedisClient) DoScan(cmd, tp, set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	retry := uint32(0)
	var err error
	var rsps []interface{}
	var conns []redis.Conn
	var wg sync.WaitGroup
	connCursorMap := make(map[string]string)
	var mutex sync.Mutex
	ns := client.conf.Namespace
	for retry < 3 {
		retry++
		if len(cursor) == 0 {
			conns, err = client.cluster.GetConnsForAllParts()
		} else {
			decodedCursor, err := base64.StdEncoding.DecodeString(string(cursor))
			if err != nil {
				return nil, nil, err
			}

			cursors := bytes.Split(decodedCursor, []byte("|"))
			if len(cursors) <= 0 {
				return nil, nil, errors.New("invalid cursor")
			}

			var hosts []string
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
			conns, err = client.cluster.GetConnsByHosts(hosts)
		}
		if err != nil {
			client.cluster.MaybeTriggerCheckForError(err, 0)
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
			continue
		}
		rsps = make([]interface{}, len(conns))
		for i, c := range conns {
			wg.Add(1)
			go func(index int, c redis.Conn) {
				defer wg.Done()
				mutex.Lock()
				cur := connCursorMap[c.RemoteAddrStr()]
				mutex.Unlock()
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
						cursor = append(cursor, []byte(c.RemoteAddrStr())...)
						cursor = append(cursor, []byte("@")...)
						cursor = append(cursor, originCursor...)
						ay[0] = cursor
					}
					rsps[index] = ay
				}
			}(i, c)
		}
		wg.Wait()
		break
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

func (client *ZanRedisClient) DoScanChannel(cmd, tp, set string, stopC chan struct{}) chan []byte {
	ch := make(chan []byte, 10)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				levelLog.Errorf("scan panic error. [err=%v]\n", err)
			}
			close(ch)
		}()
		retry := uint32(0)
		var err error
		var conns []redis.Conn
		ns := client.conf.Namespace
		for retry < 3 {
			retry++
			conns, err = client.cluster.GetConnsForAllParts()
			if err != nil {
				client.cluster.MaybeTriggerCheckForError(err, 0)
				select {
				case <-stopC:
					break
				default:
				}
				time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
				continue
			}
			var wg sync.WaitGroup
			for _, c := range conns {
				wg.Add(1)
				go func(c redis.Conn) {
					defer func() {
						wg.Done()
						if err := recover(); err != nil {
							levelLog.Errorf("scan panic error. [err=%v]\n", err)
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
							select {
							case <-stopC:
								break
							case ch <- val:
							}
						}

						if len(cursor) == 0 {
							break
						}
					}
				}(c)
			}
			wg.Wait()
			break
		}
	}()
	return ch
}

func (client *ZanRedisClient) DoFullScanChannel(tp, set string, stopC chan struct{}) chan interface{} {
	ch := make(chan interface{}, 10)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				levelLog.Errorf("scan error. [err=%v]\n", err)
			}
			close(ch)
		}()
		retry := uint32(0)
		var err error
		var conns []redis.Conn
		ns := client.conf.Namespace
		for retry < 3 {
			retry++
			conns, err = client.cluster.GetConnsForAllParts()
			if err != nil {
				client.cluster.MaybeTriggerCheckForError(err, 0)
				select {
				case <-stopC:
					break
				default:
				}
				time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
				continue
			}

			var wg sync.WaitGroup
			for _, c := range conns {
				wg.Add(1)
				go func(c redis.Conn) {
					defer func() {
						wg.Done()
						if err := recover(); err != nil {
							levelLog.Errorf("full scan error. [err=%v]\n", err)
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
						ay, err := redis.Values(c.Do("FULLSCAN", tmp.Bytes(), tp, "count", 1000))
						if err != nil {
							levelLog.Errorf("get error when DoFullScan. [err=%v]\n", err)
							break
						}
						if len(ay) != 2 {
							levelLog.Errorf("response length is not 2 when DoFullScan. [len=%d]\n", len(ay))
							break
						}

						cursor = ay[0].([]byte)
						a, err := redis.MultiBulk(ay[1], nil)
						if err != nil {
							levelLog.Errorf("get error. [Err=%v]\n", err)
							break
						}

						select {
						case <-stopC:
							break
						case ch <- a:
						}

						if len(cursor) == 0 {
							break
						}
					}
				}(c)
			}
			wg.Wait()
			break
		}
	}()
	return ch
}

func (client *ZanRedisClient) DoFullScan(cmd, tp, set string, count int, cursor []byte) ([]byte, []interface{}, error) {
	retry := uint32(0)
	var err error
	var rsps []interface{}
	var conns []redis.Conn
	var wg sync.WaitGroup
	connCursorMap := make(map[string]string)
	ns := client.conf.Namespace
	for retry < 3 {
		retry++

		if len(cursor) == 0 {
			conns, err = client.cluster.GetConnsForAllParts()
		} else {
			decodedCursor, err := base64.StdEncoding.DecodeString(string(cursor))
			if err != nil {
				return nil, nil, err
			}

			cursors := bytes.Split(decodedCursor, []byte("|"))
			if len(cursors) <= 0 {
				return nil, nil, errors.New("invalid cursor")
			}

			hosts := make([]string, 0)
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
			conns, err = client.cluster.GetConnsByHosts(hosts)
		}
		if err != nil {
			client.cluster.MaybeTriggerCheckForError(err, 0)
			time.Sleep(MIN_RETRY_SLEEP + time.Millisecond*time.Duration(10*(2<<retry)))
			continue
		}

		rsps = make([]interface{}, len(conns))
		for i, c := range conns {
			wg.Add(1)
			go func(index int, c redis.Conn) {
				defer wg.Done()
				cur := connCursorMap[c.RemoteAddrStr()]
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
						cursor = append(cursor, []byte(c.RemoteAddrStr())...)
						cursor = append(cursor, []byte("@")...)
						cursor = append(cursor, originCursor...)
						ay[0] = cursor
					}
					rsps[index] = ay
				}
			}(i, c)
		}
		wg.Wait()
		break
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

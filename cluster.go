package zanredisdb

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/spaolacci/murmur3"
)

func GetHashedPartitionID(pk []byte, pnum int) int {
	return int(murmur3.Sum32(pk)) % pnum
}

type RedisHost struct {
	addr     string
	connPool *redis.Pool
}

type PartitionInfo struct {
	Leader      string
	Replicas    []string
	chosenIndex int32
}

type Partitions struct {
	PNum  int
	PList []PartitionInfo
}

type Cluster struct {
	sync.Mutex
	lookupMtx   sync.RWMutex
	LookupList  []string
	lookupIndex int

	namespace    string
	epoch        int64
	parts        Partitions
	nodes        map[string]*RedisHost
	tendInterval int64
	wg           sync.WaitGroup
	quitC        chan struct{}
	tendTrigger  chan int

	dialF func(string) (redis.Conn, error)
}

func NewCluster(conf *Conf) *Cluster {
	self := &Cluster{
		quitC:        make(chan struct{}),
		tendTrigger:  make(chan int, 1),
		tendInterval: conf.TendInterval,
		LookupList:   make([]string, len(conf.LookupList)),
		nodes:        make(map[string]*RedisHost),
		namespace:    conf.Namespace,
	}
	if self.tendInterval <= 0 {
		panic("tend interval should be great than zero")
	}

	copy(self.LookupList, conf.LookupList)

	self.dialF = func(addr string) (redis.Conn, error) {
		return redis.Dial("tcp", addr, redis.DialConnectTimeout(conf.DialTimeout*time.Second),
			redis.DialReadTimeout(conf.ReadTimeout),
			redis.DialWriteTimeout(conf.WriteTimeout),
			redis.DialPassword(conf.Password),
		)
	}

	self.tend()

	if len(self.nodes) == 0 {
		levelLog.Errorln("no node in server list is available at init")
	}

	self.wg.Add(1)

	go self.tendNodes()

	return self
}

func (self *Cluster) MaybeTriggerCheckForError(err error, delay time.Duration) bool {
	if err == nil {
		return false
	}
	if IsConnectRefused(err) || IsFailedOnClusterChanged(err) {
		time.Sleep(delay)
		select {
		case self.tendTrigger <- 1:
			levelLog.Infof("trigger tend for err: %v", err)
		default:
		}
		return true
	}
	return false
}

func (self *Cluster) GetNodePool(pk []byte, leader bool) (*redis.Pool, error) {
	self.Lock()
	defer self.Unlock()

	if len(self.nodes) == 0 {
		return nil, errors.New("no server is available right now")
	}
	if self.parts.PNum == 0 || len(self.parts.PList) == 0 {
		return nil, errors.New("no any partition")
	}
	pid := GetHashedPartitionID(pk, self.parts.PNum)
	part := self.parts.PList[pid]
	picked := ""
	if leader {
		picked = part.Leader
	} else {
		if len(part.Replicas) == 0 {
			return nil, errors.New("no any replica for partition")
		}
		picked = part.Replicas[int(atomic.AddInt32(&part.chosenIndex, 1))%len(part.Replicas)]
	}

	if picked == "" {
		return nil, errNoNodeForPartition
	}
	node, ok := self.nodes[picked]
	if !ok {
		levelLog.Infof("node %v not found", picked)
		return nil, errors.New("node not found")
	}
	levelLog.Detailf("node %v chosen", picked)
	return node.connPool, nil
}

func (self *Cluster) GetConn(pk []byte, leader bool) (redis.Conn, error) {
	connPool, err := self.GetNodePool(pk, leader)
	if err != nil {
		return nil, err
	}
	conn := connPool.Get()
	return conn, nil
}

func (self *Cluster) nextLookupEndpoint() (string, string) {
	self.lookupMtx.RLock()
	if self.lookupIndex >= len(self.LookupList) {
		self.lookupIndex = 0
	}
	addr := self.LookupList[self.lookupIndex]
	num := len(self.LookupList)
	self.lookupIndex = (self.lookupIndex + 1) % num
	self.lookupMtx.RUnlock()

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	listUrl := *u
	if u.Path == "/" || u.Path == "" {
		u.Path = "/query/" + self.namespace
	}
	listUrl.Path = "/listpd"

	tmpUrl := *u
	v, _ := url.ParseQuery(tmpUrl.RawQuery)
	v.Add("epoch", strconv.FormatInt(self.epoch, 10))
	tmpUrl.RawQuery = v.Encode()
	return tmpUrl.String(), listUrl.String()
}

func (self *Cluster) tend() {
	queryStr, discoveryUrl := self.nextLookupEndpoint()
	// discovery other lookupd nodes from current lookupd or from etcd
	levelLog.Debugf("discovery nsqlookupd %s", discoveryUrl)
	var listResp listPDResp
	_, err := apiRequest("GET", discoveryUrl, nil, &listResp)
	if err != nil {
		levelLog.Warningf("error discovery lookup (%s) - %s", discoveryUrl, err)
	} else {
		for _, node := range listResp.PDNodes {
			addr := net.JoinHostPort(node.NodeIP, node.HttpPort)
			self.lookupMtx.Lock()
			found := false
			for _, x := range self.LookupList {
				if x == addr {
					found = true
					break
				}
			}
			if !found {
				self.LookupList = append(self.LookupList, addr)
				levelLog.Debugf("new lookup added %v", addr)
			}
			self.lookupMtx.Unlock()
		}
	}

	levelLog.Debugf("querying for namespace %s", queryStr)
	var data queryNamespaceResp
	statusCode, err := apiRequest("GET", queryStr, nil, &data)
	if err != nil {
		if statusCode != http.StatusNotModified {
			levelLog.Warningf("error querying (%s) - %s", queryStr, err)
		} else {
			levelLog.Debugf("server return unchanged, local %v", atomic.LoadInt64(&self.epoch))
		}
		return
	}

	if len(data.Partitions) != data.PartitionNum {
		levelLog.Warningf("response on partitions mismatch: %v", data)
		return
	}
	newPartitions := Partitions{PNum: data.PartitionNum, PList: make([]PartitionInfo, data.PartitionNum)}
	if data.Epoch == atomic.LoadInt64(&self.epoch) {
		levelLog.Debugf("namespace info keep unchanged: %v", data)
		return
	}
	for partID, partNodeInfo := range data.Partitions {
		if partID >= newPartitions.PNum || partID < 0 {
			levelLog.Errorf("got invalid partition : %v", partID)
			return
		}
		node := partNodeInfo.Leader
		leaderAddr := ""
		if node.BroadcastAddress != "" {
			leaderAddr = net.JoinHostPort(node.BroadcastAddress, node.RedisPort)
		} else {
			levelLog.Infof("partition %v missing leader node", partID)
		}
		replicas := make([]string, 0)
		for _, n := range partNodeInfo.Replicas {
			if n.BroadcastAddress != "" {
				addr := net.JoinHostPort(n.BroadcastAddress, n.RedisPort)
				replicas = append(replicas, addr)
			}
		}
		newPartitions.PList[partID] = PartitionInfo{Leader: leaderAddr, Replicas: replicas}
	}
	cleanHosts := make(map[string]*RedisHost)
	levelLog.Debugf("namespace %v partitions changed to : %v", self.namespace, newPartitions)
	self.Lock()
	if len(newPartitions.PList) > 0 {
		for k, p := range self.nodes {
			found := false
			for _, partInfo := range newPartitions.PList {
				if p.addr == partInfo.Leader {
					found = true
					break
				}
				for _, replica := range partInfo.Replicas {
					if p.addr == replica {
						found = true
						break
					}
				}
				if found {
					break
				}
			}
			if !found {
				levelLog.Infof("node %v for namespace %v removing since not in lookup", p.addr, self.namespace)
				cleanHosts[k] = p
				delete(self.nodes, k)
			}
		}
	}
	self.parts = newPartitions

	testF := func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > 60*time.Second {
			_, err = c.Do("PING")
		}
		return
	}

	for _, partInfo := range newPartitions.PList {
		for _, replica := range partInfo.Replicas {
			_, ok := self.nodes[replica]
			if ok {
				continue
			}
			newNode := &RedisHost{addr: replica}
			newNode.connPool = &redis.Pool{
				MaxIdle:      10,
				MaxActive:    100,
				IdleTimeout:  120 * time.Second,
				TestOnBorrow: testF,
				Dial:         func() (redis.Conn, error) { return self.dialF(newNode.addr) },
			}
			levelLog.Infof("host:%v is available and come into service", newNode.addr)
			self.nodes[replica] = newNode
		}
	}
	atomic.StoreInt64(&self.epoch, data.Epoch)
	self.Unlock()
	for _, p := range cleanHosts {
		p.connPool.Close()
	}
}

func (self *Cluster) tendNodes() {
	tendTicker := time.NewTicker(time.Duration(self.tendInterval) * time.Second)
	defer func() {
		tendTicker.Stop()
		self.wg.Done()
	}()

	for {
		select {
		case <-tendTicker.C:
			self.tend()
		case <-self.tendTrigger:
			levelLog.Infof("trigger tend")
			self.tend()
			time.Sleep(time.Millisecond * 100)
		case <-self.quitC:
			self.Lock()
			nodes := self.nodes
			self.nodes = make(map[string]*RedisHost)
			self.Unlock()
			for _, node := range nodes {
				node.connPool.Close()
			}
			levelLog.Debugf("go routine for tend self exit")
			return
		}
	}
}

func (self *Cluster) Close() {
	close(self.quitC)
	self.wg.Wait()
	levelLog.Debugf("cluster exit")
}

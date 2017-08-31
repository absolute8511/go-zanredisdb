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

func FindString(src []string, f string) int {
	for i, v := range src {
		if f == v {
			return i
		}
	}
	return -1
}

type Cluster struct {
	sync.Mutex
	conf        *Conf
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
		conf:         conf,
	}
	if self.tendInterval <= 0 {
		panic("tend interval should be great than zero")
	}

	copy(self.LookupList, conf.LookupList)

	self.dialF = func(addr string) (redis.Conn, error) {
		return redis.Dial("tcp", addr, redis.DialConnectTimeout(conf.DialTimeout),
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
		if delay > 0 {
			time.Sleep(delay)
		}
		select {
		case self.tendTrigger <- 1:
			levelLog.Infof("trigger tend for err: %v", err)
		default:
		}
		return true
	}
	return false
}

func (self *Cluster) GetPartitionNum() int {
	defer self.Unlock()
	self.Lock()
	return self.parts.PNum
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
	levelLog.Detailf("node %v chosen for partition id: %v, pk: %s", picked, pid, string(pk))
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

func (cluster *Cluster) getConnsByHosts(hosts []string) ([]redis.Conn, []string, error) {
	if len(cluster.nodes) == 0 {
		return nil, nil, errors.New("no server is available right now")
	}
	if cluster.parts.PNum == 0 || len(cluster.parts.PList) == 0 {
		return nil, nil, errors.New("no any partition")
	}
	var conns []redis.Conn
	for i, h := range hosts {
		if v, ok := cluster.nodes[h]; ok {
			conn := v.connPool.Get()
			conns = append(conns, conn)
		} else {
			hosts = append(hosts[:i], hosts[i+1:]...)
		}
	}
	if len(conns) == 0 {
		return nil, nil, errNoNodeForPartition
	}
	return conns, hosts, nil
}

func (cluster *Cluster) GetConns() ([]redis.Conn, []string, error) {
	cluster.Lock()
	defer cluster.Unlock()

	var hosts []string
	for _, p := range cluster.parts.PList {
		hosts = append(hosts, p.Leader)
	}
	return cluster.getConnsByHosts(hosts)
}

func (cluster *Cluster) GetConnsByHosts(hosts []string) ([]redis.Conn, []string, error) {
	cluster.Lock()
	defer cluster.Unlock()
	return cluster.getConnsByHosts(hosts)
}

func (self *Cluster) nextLookupEndpoint() (string, string, string) {
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
	return addr, tmpUrl.String(), listUrl.String()
}

func (self *Cluster) tend() {
	addr, queryStr, discoveryUrl := self.nextLookupEndpoint()
	// discovery other lookupd nodes from current lookupd or from etcd
	levelLog.Debugf("discovery lookupd %s", discoveryUrl)
	var listResp listPDResp
	httpRespCode, err := apiRequest("GET", discoveryUrl, nil, &listResp)
	if err != nil {
		levelLog.Warningf("error discovery lookup (%s) - %s", discoveryUrl, err)
		if httpRespCode < 0 {
			self.lookupMtx.Lock()
			// remove failed if not seed nodes
			if FindString(self.conf.LookupList, addr) == -1 {
				levelLog.Infof("removing failed lookup : %v", addr)
				newLookupList := make([]string, 0)
				for _, v := range self.LookupList {
					if v == addr {
						continue
					} else {
						newLookupList = append(newLookupList, v)
					}
				}
				self.LookupList = newLookupList
			}
			self.lookupMtx.Unlock()
		}
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
	if data.Epoch < atomic.LoadInt64(&self.epoch) {
		levelLog.Infof("namespace info is older: %v vs %v", data.Epoch, atomic.LoadInt64(&self.epoch))
		return
	}
	self.Lock()
	oldPartitions := self.parts
	self.Unlock()

	for partID, partNodeInfo := range data.Partitions {
		if partID >= newPartitions.PNum || partID < 0 {
			levelLog.Errorf("got invalid partition : %v", partID)
			return
		}
		node := partNodeInfo.Leader
		leaderAddr := ""
		var oldPartInfo PartitionInfo
		if partID < len(oldPartitions.PList) {
			oldPartInfo = oldPartitions.PList[partID]
		}
		if node.BroadcastAddress != "" {
			leaderAddr = net.JoinHostPort(node.BroadcastAddress, node.RedisPort)
		} else {
			levelLog.Infof("partition %v missing leader node, use old instead", partID, oldPartInfo.Leader)
			leaderAddr = oldPartInfo.Leader
		}
		replicas := make([]string, 0)
		for _, n := range partNodeInfo.Replicas {
			if n.BroadcastAddress != "" {
				addr := net.JoinHostPort(n.BroadcastAddress, n.RedisPort)
				replicas = append(replicas, addr)
			}
		}
		if len(replicas) == 0 {
			replicas = oldPartInfo.Replicas
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
				MaxIdle:      self.conf.MaxIdleConn,
				MaxActive:    self.conf.MaxActiveConn,
				IdleTimeout:  120 * time.Second,
				TestOnBorrow: testF,
				Dial:         func() (redis.Conn, error) { return self.dialF(newNode.addr) },
			}
			if self.conf.IdleTimeout > time.Second {
				newNode.connPool.IdleTimeout = self.conf.IdleTimeout
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

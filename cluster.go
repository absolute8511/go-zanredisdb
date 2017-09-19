package zanredisdb

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redigo/redis"
	"github.com/spaolacci/murmur3"
)

const (
	DEFAULT_CONN_POOL_SIZE = 100
)

var (
	errNoAnyPartitions = errors.New("no any partitions")
)

func GetHashedPartitionID(pk []byte, pnum int) int {
	return int(murmur3.Sum32(pk)) % pnum
}

type RedisHost struct {
	addr     string
	connPool *redis.QueuePool
}

type PartitionInfo struct {
	Leader      *RedisHost
	Replicas    []*RedisHost
	chosenIndex int32
}

type PartitionAddrInfo struct {
	Leader      string
	Replicas    []string
	chosenIndex int32
}

func (pi *PartitionInfo) clone() PartitionInfo {
	var cloned PartitionInfo
	cloned.Leader = pi.Leader
	cloned.Replicas = make([]*RedisHost, 0, len(pi.Replicas))
	cloned.chosenIndex = pi.chosenIndex
	for _, v := range pi.Replicas {
		cloned.Replicas = append(cloned.Replicas, v)
	}
	return cloned
}

type Partitions struct {
	PNum  int
	Epoch int64
	PList []PartitionInfo
}

type PartitionAddrs struct {
	PNum  int
	PList []PartitionAddrInfo
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
	sync.RWMutex
	conf        *Conf
	lookupMtx   sync.RWMutex
	LookupList  []string
	lookupIndex int

	namespace string
	//parts        Partitions
	parts atomic.Value
	//nodes        map[string]*RedisHost
	tendInterval int64
	wg           sync.WaitGroup
	quitC        chan struct{}
	tendTrigger  chan int

	dialF func(string) (redis.Conn, error)
}

func NewCluster(conf *Conf) *Cluster {
	cluster := &Cluster{
		quitC:        make(chan struct{}),
		tendTrigger:  make(chan int, 1),
		tendInterval: conf.TendInterval,
		LookupList:   make([]string, len(conf.LookupList)),
		//nodes:        make(map[string]*RedisHost),
		namespace: conf.Namespace,
		conf:      conf,
	}
	if cluster.tendInterval <= 0 {
		panic("tend interval should be great than zero")
	}

	copy(cluster.LookupList, conf.LookupList)

	cluster.dialF = func(addr string) (redis.Conn, error) {
		return redis.Dial("tcp", addr, redis.DialConnectTimeout(conf.DialTimeout),
			redis.DialReadTimeout(conf.ReadTimeout),
			redis.DialWriteTimeout(conf.WriteTimeout),
			redis.DialPassword(conf.Password),
		)
	}

	cluster.tend()

	//if len(cluster.nodes) == 0 {
	//		levelLog.Errorln("no node in server list is available at init")
	//	}

	cluster.wg.Add(1)

	go cluster.tendNodes()

	return cluster
}

func (cluster *Cluster) MaybeTriggerCheckForError(err error, delay time.Duration) bool {
	if err == nil {
		return false
	}
	if IsConnectRefused(err) || IsFailedOnClusterChanged(err) {
		if delay > 0 {
			time.Sleep(delay)
		}
		select {
		case cluster.tendTrigger <- 1:
			levelLog.Infof("trigger tend for err: %v", err)
		default:
		}
		return true
	}
	return false
}

func (cluster *Cluster) getPartitions() *Partitions {
	p := cluster.parts.Load()
	if p == nil {
		return &Partitions{}
	}
	return p.(*Partitions)
}

func (cluster *Cluster) setPartitions(p *Partitions) {
	cluster.parts.Store(p)
}

func getNodesFromParts(parts *Partitions) map[string]*RedisHost {
	nodes := make(map[string]*RedisHost, parts.PNum)
	for _, v := range parts.PList {
		if v.Leader != nil {
			nodes[v.Leader.addr] = v.Leader
		}
		for _, r := range v.Replicas {
			nodes[r.addr] = r
		}
	}
	return nodes
}

func (cluster *Cluster) GetPartitionNum() int {
	return cluster.getPartitions().PNum
}

func (cluster *Cluster) GetNodePool(pk []byte, leader bool) (*redis.QueuePool, error) {
	parts := cluster.getPartitions()
	if parts == nil {
		return nil, errNoAnyPartitions
	}
	if parts.PNum == 0 || len(parts.PList) == 0 {
		return nil, errNoAnyPartitions
	}
	pid := GetHashedPartitionID(pk, parts.PNum)
	part := parts.PList[pid]
	var picked *RedisHost
	if leader {
		picked = part.Leader
	} else {
		if len(part.Replicas) == 0 {
			return nil, errors.New("no any replica for partition")
		}
		picked = part.Replicas[int(atomic.AddInt32(&part.chosenIndex, 1))%len(part.Replicas)]
	}

	if picked == nil {
		return nil, errNoNodeForPartition
	}
	if levelLog.Level() > 2 {
		levelLog.Detailf("node %v chosen for partition id: %v, pk: %s", picked.addr, pid, string(pk))
	}
	return picked.connPool, nil
}

func (cluster *Cluster) GetConn(pk []byte, leader bool) (redis.Conn, error) {
	connPool, err := cluster.GetNodePool(pk, leader)
	if err != nil {
		return nil, err
	}
	conn, err := connPool.Get(0)
	return conn, err
}

func (cluster *Cluster) getConnsByHosts(hosts []string) ([]redis.Conn, error) {
	parts := cluster.getPartitions()
	if parts == nil {
		return nil, errNoAnyPartitions
	}

	nodes := getNodesFromParts(parts)
	var conns []redis.Conn
	for _, h := range hosts {
		if v, ok := nodes[h]; ok {
			conn, err := v.connPool.Get(0)
			if err != nil {
				return nil, err
			}
			conns = append(conns, conn)
		} else {
			return nil, fmt.Errorf("node %v not found while get connection", h)
		}
	}
	return conns, nil
}

func (cluster *Cluster) GetConnsForAllParts() ([]redis.Conn, error) {
	parts := cluster.getPartitions()
	if parts == nil {
		return nil, errNoAnyPartitions
	}
	if parts.PNum == 0 || len(parts.PList) == 0 {
		return nil, errNoNodeForPartition
	}
	var conns []redis.Conn
	for _, p := range parts.PList {
		if p.Leader == nil {
			return nil, errors.New("no leader for partition")
		}
		conn, err := p.Leader.connPool.Get(0)
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}
	return conns, nil
}

func (cluster *Cluster) GetConnsByHosts(hosts []string) ([]redis.Conn, error) {
	return cluster.getConnsByHosts(hosts)
}

func (cluster *Cluster) nextLookupEndpoint(epoch int64) (string, string, string) {
	cluster.lookupMtx.RLock()
	if cluster.lookupIndex >= len(cluster.LookupList) {
		cluster.lookupIndex = 0
	}
	addr := cluster.LookupList[cluster.lookupIndex]
	num := len(cluster.LookupList)
	cluster.lookupIndex = (cluster.lookupIndex + 1) % num
	cluster.lookupMtx.RUnlock()

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
		u.Path = "/query/" + cluster.namespace
	}
	listUrl.Path = "/listpd"

	tmpUrl := *u
	v, _ := url.ParseQuery(tmpUrl.RawQuery)
	v.Add("epoch", strconv.FormatInt(epoch, 10))
	tmpUrl.RawQuery = v.Encode()
	return addr, tmpUrl.String(), listUrl.String()
}

func (cluster *Cluster) tend() {
	oldPartitions := cluster.getPartitions()
	oldEpoch := oldPartitions.Epoch

	addr, queryStr, discoveryUrl := cluster.nextLookupEndpoint(oldEpoch)
	// discovery other lookupd nodes from current lookupd or from etcd
	levelLog.Debugf("discovery lookupd %s", discoveryUrl)
	var listResp listPDResp
	httpRespCode, err := apiRequest("GET", discoveryUrl, nil, &listResp)
	if err != nil {
		levelLog.Warningf("error discovery lookup (%s) - %s, code: %v", discoveryUrl, err, httpRespCode)
		if httpRespCode < 0 {
			cluster.lookupMtx.Lock()
			// remove failed if not seed nodes
			if FindString(cluster.conf.LookupList, addr) == -1 {
				levelLog.Infof("removing failed lookup : %v", addr)
				newLookupList := make([]string, 0)
				for _, v := range cluster.LookupList {
					if v == addr {
						continue
					} else {
						newLookupList = append(newLookupList, v)
					}
				}
				cluster.LookupList = newLookupList
			}
			cluster.lookupMtx.Unlock()
			select {
			case cluster.tendTrigger <- 1:
				levelLog.Infof("trigger tend for err: %v", err)
			default:
			}
			return
		}
	} else {
		for _, node := range listResp.PDNodes {
			addr := net.JoinHostPort(node.NodeIP, node.HttpPort)
			cluster.lookupMtx.Lock()
			found := false
			for _, x := range cluster.LookupList {
				if x == addr {
					found = true
					break
				}
			}
			if !found {
				cluster.LookupList = append(cluster.LookupList, addr)
				levelLog.Infof("new lookup added %v", addr)
			}
			cluster.lookupMtx.Unlock()
		}
	}

	levelLog.Debugf("querying for namespace %s", queryStr)
	var data queryNamespaceResp
	statusCode, err := apiRequest("GET", queryStr, nil, &data)
	if err != nil {
		if statusCode != http.StatusNotModified {
			levelLog.Warningf("error querying (%s) - %s", queryStr, err)
		} else {
			levelLog.Debugf("server return unchanged, local %v", oldEpoch)
		}
		return
	}

	if len(data.Partitions) != data.PartitionNum {
		levelLog.Warningf("response on partitions mismatch: %v", data)
		return
	}
	newPartitions := PartitionAddrs{PNum: data.PartitionNum, PList: make([]PartitionAddrInfo, data.PartitionNum)}
	if data.Epoch == oldEpoch {
		levelLog.Debugf("namespace info keep unchanged: %v", data)
		return
	}
	if data.Epoch < oldEpoch {
		levelLog.Infof("namespace info is older: %v vs %v", data.Epoch, oldEpoch)
		return
	}

	for partID, partNodeInfo := range data.Partitions {
		if partID >= newPartitions.PNum || partID < 0 {
			levelLog.Errorf("got invalid partition : %v", partID)
			return
		}
		node := partNodeInfo.Leader
		var leaderAddr string
		var oldPartInfo PartitionInfo
		var oldLeader string
		if partID < len(oldPartitions.PList) {
			oldPartInfo = oldPartitions.PList[partID]
			if oldPartInfo.Leader != nil {
				oldLeader = oldPartInfo.Leader.addr
			}
		}
		if node.BroadcastAddress != "" {
			leaderAddr = net.JoinHostPort(node.BroadcastAddress, node.RedisPort)
		} else {
			levelLog.Infof("partition %v missing leader node, use old instead", partID, oldPartInfo.Leader)
			leaderAddr = oldLeader
		}
		if oldLeader != leaderAddr {
			levelLog.Infof("partition %v leader changed from %v to %v", partID, oldLeader, leaderAddr)
		}
		replicas := make([]string, 0)
		for _, n := range partNodeInfo.Replicas {
			if n.BroadcastAddress != "" {
				addr := net.JoinHostPort(n.BroadcastAddress, n.RedisPort)
				replicas = append(replicas, addr)
			}
		}
		if len(replicas) == 0 {
			for _, r := range oldPartInfo.Replicas {
				replicas = append(replicas, r.addr)
			}
		}
		pinfo := PartitionAddrInfo{Leader: leaderAddr, Replicas: replicas}
		pinfo.chosenIndex = oldPartInfo.chosenIndex
		newPartitions.PList[partID] = pinfo
		levelLog.Infof("namespace %v partition %v replicas changed to : %v", cluster.namespace, partID, pinfo)
	}
	cleanHosts := make(map[string]*RedisHost)
	nodes := getNodesFromParts(oldPartitions)
	if len(newPartitions.PList) > 0 {
		for k, p := range nodes {
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
				levelLog.Infof("node %v for namespace %v removing since not in lookup", p.addr, cluster.namespace)
				cleanHosts[k] = p
				delete(nodes, k)
			}
		}
	}

	testF := func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > 60*time.Second {
			_, err = c.Do("PING")
		}
		return
	}

	for _, partInfo := range newPartitions.PList {
		for _, replica := range partInfo.Replicas {
			_, ok := nodes[replica]
			if ok {
				continue
			}
			newNode := &RedisHost{addr: replica}
			maxActive := DEFAULT_CONN_POOL_SIZE
			if cluster.conf.MaxActiveConn > 0 {
				maxActive = cluster.conf.MaxActiveConn
			}
			newNode.connPool = redis.NewQueuePool(func() (redis.Conn, error) { return cluster.dialF(newNode.addr) },
				cluster.conf.MaxIdleConn, maxActive)
			newNode.connPool.IdleTimeout = 120 * time.Second
			newNode.connPool.TestOnBorrow = testF
			if cluster.conf.IdleTimeout > time.Second {
				newNode.connPool.IdleTimeout = cluster.conf.IdleTimeout
			}
			levelLog.Infof("host:%v is available and come into service", newNode.addr)
			nodes[replica] = newNode
		}
	}
	newHostPartitions := &Partitions{PNum: newPartitions.PNum, Epoch: data.Epoch,
		PList: make([]PartitionInfo, 0, len(newPartitions.PList))}

	for _, partInfo := range newPartitions.PList {
		var pi PartitionInfo
		pi.chosenIndex = partInfo.chosenIndex
		var ok bool
		pi.Leader, ok = nodes[partInfo.Leader]
		if !ok {
			levelLog.Infof("host:%v not found ", partInfo.Leader)
		}
		for _, r := range partInfo.Replicas {
			n, ok := nodes[r]
			if !ok || n == nil {
				levelLog.Infof("host:%v not found ", r)
				continue
			}
			pi.Replicas = append(pi.Replicas, n)
		}
		newHostPartitions.PList = append(newHostPartitions.PList, pi)
	}
	cluster.setPartitions(newHostPartitions)
	for _, p := range cleanHosts {
		p.connPool.Close()
	}
}

func (cluster *Cluster) tendNodes() {
	tendTicker := time.NewTicker(time.Duration(cluster.tendInterval) * time.Second)
	defer func() {
		tendTicker.Stop()
		cluster.wg.Done()
	}()

	for {
		select {
		case <-tendTicker.C:
			cluster.tend()

			nodes := getNodesFromParts(cluster.getPartitions())
			for _, n := range nodes {
				n.connPool.Refresh()
			}

		case <-cluster.tendTrigger:
			levelLog.Infof("trigger tend")
			cluster.tend()
			time.Sleep(MIN_RETRY_SLEEP / 2)
		case <-cluster.quitC:
			nodes := getNodesFromParts(cluster.getPartitions())
			for _, node := range nodes {
				node.connPool.Close()
			}
			levelLog.Debugf("go routine for tend cluster exit")
			return
		}
	}
}

func (cluster *Cluster) Close() {
	close(cluster.quitC)
	cluster.wg.Wait()
	levelLog.Debugf("cluster exit")
}

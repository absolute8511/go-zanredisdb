package main

import (
	"flag"
	"fmt"
	"github.com/absolute8511/go-zanredisdb"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ip = flag.String("ip", "127.0.0.1", "pd server ip")
var port = flag.Int("port", 18001, "pd server port")
var number = flag.Int("n", 1000, "request number")
var clients = flag.Int("c", 10, "number of clients")
var round = flag.Int("r", 1, "benchmark round number")
var valueSize = flag.Int("vsize", 100, "kv value size")
var tests = flag.String("t", "set,get,randget,del", "only run the comma separated list of tests")
var primaryKeyCnt = flag.Int("pkn", 100, "primary key count for hash,list,set,zset")
var namespace = flag.String("namespace", "default", "the prefix namespace")
var table = flag.String("table", "test", "the table to write")
var maxExpireSecs = flag.Int("maxExpire", 60, "max expire seconds to be allowed with setex")
var minExpireSecs = flag.Int("minExpire", 10, "min expire seconds to be allowed with setex")

var wg sync.WaitGroup
var loop int = 0
var latencyDistribute []int64

func doCommand(client *zanredisdb.ZanRedisClient, cmd string, args ...interface{}) error {
	v := args[0]
	prefix := *namespace + ":" + *table + ":"
	sharding := ""
	switch vt := v.(type) {
	case string:
		sharding = *table + ":" + vt
		args[0] = prefix + vt
	case []byte:
		sharding = *table + ":" + string(vt)
		args[0] = []byte(prefix + string(vt))
	case int:
		sharding = *table + ":" + strconv.Itoa(vt)
		args[0] = prefix + strconv.Itoa(vt)
	case int64:
		sharding = *table + ":" + strconv.Itoa(int(vt))
		args[0] = prefix + strconv.Itoa(int(vt))
	}
	s := time.Now()
	_, err := client.DoRedis(strings.ToUpper(cmd), []byte(sharding), true, args...)
	if err != nil {
		fmt.Printf("do %s (%v) error %s\n", cmd, args[0], err.Error())
		return err
	}
	cost := time.Since(s).Nanoseconds()
	index := cost / 1000 / 1000
	if index < 100 {
		index = index / 10
	} else if index < 1000 {
		index = 9 + index/100
	} else if index < 10000 {
		index = 19 + index/1000
	} else {
		index = 29
	}
	atomic.AddInt64(&latencyDistribute[index], 1)
	return nil
}

func bench(cmd string, f func(c *zanredisdb.ZanRedisClient) error) {
	wg.Add(*clients)

	t1 := time.Now()
	pdAddr := fmt.Sprintf("%s:%d", *ip, *port)
	currentNumList := make([]int64, *clients)
	errCnt := int64(0)
	done := int32(0)
	for i := 0; i < *clients; i++ {
		go func(clientIndex int) {
			var err error
			conf := &zanredisdb.Conf{
				DialTimeout:  time.Second * 15,
				ReadTimeout:  0,
				WriteTimeout: 0,
				TendInterval: 10,
				Namespace:    *namespace,
			}
			conf.LookupList = append(conf.LookupList, pdAddr)
			c := zanredisdb.NewZanRedisClient(conf)
			c.Start()
			for j := 0; j < loop; j++ {
				err = f(c)
				if err != nil {
					atomic.AddInt64(&errCnt, 1)
				}
				atomic.AddInt64(&currentNumList[clientIndex], 1)
			}
			c.Stop()
			wg.Done()
		}(i)
	}
	go func() {
		lastNum := int64(0)
		lastTime := time.Now()
		for atomic.LoadInt32(&done) == 0 {
			time.Sleep(time.Second * 30)
			t2 := time.Now()
			d := t2.Sub(lastTime)
			num := int64(0)
			for i, _ := range currentNumList {
				num += atomic.LoadInt64(&currentNumList[i])
			}
			if num <= lastNum {
				continue
			}
			fmt.Printf("%s: %s %0.3f micros/op, %0.2fop/s, err: %v, num:%v\n",
				cmd,
				d.String(),
				float64(d.Nanoseconds()/1e3)/float64(num-lastNum),
				float64(num-lastNum)/d.Seconds(),
				atomic.LoadInt64(&errCnt),
				num,
			)
			lastNum = num
			lastTime = t2
		}
	}()

	wg.Wait()
	atomic.StoreInt32(&done, 1)
	t2 := time.Now()
	d := t2.Sub(t1)

	fmt.Printf("%s: %s %0.3f micros/op, %0.2fop/s, err: %v, num:%v\n",
		cmd,
		d.String(),
		float64(d.Nanoseconds()/1e3)/float64(*number),
		float64(*number)/d.Seconds(),
		atomic.LoadInt64(&errCnt),
		*number,
	)
	for i, v := range latencyDistribute {
		if i == 0 {
			fmt.Printf("latency below 100ms\n")
		} else if i == 10 {
			fmt.Printf("latency between 100ms ~ 999ms\n")
		} else if i == 20 {
			fmt.Printf("latency above 1s\n")
		}
		fmt.Printf("latency interval %d: %v\n", i, v)
	}
}

var kvSetBase int64 = 0
var kvGetBase int64 = 0
var kvIncrBase int64 = 0
var kvDelBase int64 = 0

func benchSet() {
	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}
	magicIdentify := make([]byte, 9+3+3)
	for i := 0; i < len(magicIdentify); i++ {
		if i < 3 || i > len(magicIdentify)-3 {
			magicIdentify[i] = 0
		} else {
			magicIdentify[i] = byte(i % 3)
		}
	}
	f := func(c *zanredisdb.ZanRedisClient) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)
		n := atomic.AddInt64(&kvSetBase, 1)
		tmp := fmt.Sprintf("%010d", int(n))
		ts := time.Now().String()
		index := 0
		copy(value[index:], magicIdentify)
		index += len(magicIdentify)
		if index < *valueSize {
			copy(value[index:], ts)
			index += len(ts)
		}
		if index < *valueSize {
			copy(value[index:], tmp)
			index += len(tmp)
		}
		if *valueSize > len(magicIdentify) {
			copy(value[len(value)-len(magicIdentify):], magicIdentify)
		}
		return doCommand(c, "SET", tmp, value)
	}
	bench("set", f)
}

func benchSetEx() {
	atomic.StoreInt64(&kvSetBase, 0)

	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}
	magicIdentify := make([]byte, 9+3+3)
	for i := 0; i < len(magicIdentify); i++ {
		if i < 3 || i > len(magicIdentify)-3 {
			magicIdentify[i] = 0
		} else {
			magicIdentify[i] = byte(i % 3)
		}
	}
	f := func(c *zanredisdb.ZanRedisClient) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)
		n := atomic.AddInt64(&kvSetBase, 1)
		ttl := rand.Int31n(int32(*maxExpireSecs-*minExpireSecs)) + int32(*minExpireSecs)
		tmp := fmt.Sprintf("%010d-%d-%s", int(n), ttl, time.Now().String())
		ts := time.Now().String()
		index := 0
		copy(value[index:], magicIdentify)
		index += len(magicIdentify)
		if index < *valueSize {
			copy(value[index:], ts)
			index += len(ts)
		}
		if index < *valueSize {
			copy(value[index:], tmp)
			index += len(tmp)
		}
		if *valueSize > len(magicIdentify) {
			copy(value[len(value)-len(magicIdentify):], magicIdentify)
		}
		return doCommand(c, "SETEX", tmp, ttl, value)
	}

	bench("setex", f)
}

func benchGet() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&kvGetBase, 1)
		k := fmt.Sprintf("%010d", int(n))
		return doCommand(c, "GET", k)
	}
	bench("get", f)
}

func benchRandGet() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := rand.Int() % *number
		k := fmt.Sprintf("%010d", int(n))
		return doCommand(c, "GET", k)
	}
	bench("randget", f)
}

func benchDel() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&kvDelBase, 1)
		k := fmt.Sprintf("%010d", int(n))
		return doCommand(c, "DEL", k)
	}

	bench("del", f)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *number <= 0 {
		panic("invalid number")
		return
	}

	if *clients <= 0 || *number < *clients {
		panic("invalid client number")
		return
	}

	loop = *number / *clients

	if *round <= 0 {
		*round = 1
	}

	zanredisdb.SetLogger(1, zanredisdb.NewSimpleLogger())
	ts := strings.Split(*tests, ",")
	for i := 0; i < *round; i++ {
		for _, s := range ts {
			switch strings.ToLower(s) {
			case "set":
				benchSet()
			case "setex":
				benchSetEx()
			case "get":
				benchGet()
			case "randget":
				benchRandGet()
			case "del":
				benchDel()
			}
		}
		println("")
	}
}

func init() {
	latencyDistribute = make([]int64, 32)
}

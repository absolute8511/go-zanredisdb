package zanredisdb

import (
	"bytes"
	"fmt"
)

type ScanKey struct {
	Namespace string
	Set       string
	Cursor    []byte
	RawKey    []byte
}

func NewScanKey(ns string, set string, cursor []byte) *ScanKey {
	var tmp bytes.Buffer
	tmp.WriteString(ns)
	tmp.WriteString(":")
	tmp.WriteString(set)
	tmp.WriteString(":")
	tmp.Write(cursor)
	return &ScanKey{
		Namespace: ns,
		Set:       set,
		Cursor:    cursor,
		RawKey:    tmp.Bytes(),
	}
}

func (self *ScanKey) ShardingKey() []byte {
	return self.RawKey[len(self.Namespace)+1:]
}

func (self *ScanKey) String() string {
	return fmt.Sprintf("%s", string(self.RawKey))
}

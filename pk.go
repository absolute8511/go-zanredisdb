package zanredisdb

import (
	"bytes"
	"fmt"
)

type PKey struct {
	Namespace string
	Set       string
	PK        []byte
	RawKey    []byte
}

func NewPKey(ns string, set string, pk []byte) *PKey {
	var tmp bytes.Buffer
	tmp.WriteString(ns)
	tmp.WriteString(":")
	tmp.WriteString(set)
	tmp.WriteString(":")
	tmp.Write(pk)
	return &PKey{
		Namespace: ns,
		Set:       set,
		PK:        pk,
		RawKey:    tmp.Bytes(),
	}
}

func (self *PKey) ShardingKey() []byte {
	return self.RawKey[len(self.Namespace)+1:]
}

func (self *PKey) String() string {
	return fmt.Sprintf("%s", string(self.RawKey))
}

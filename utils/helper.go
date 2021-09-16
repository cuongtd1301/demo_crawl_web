package utils

import (
	"encoding/binary"

	"github.com/google/uuid"
)

func GenUuid() int64 {
	u1 := uuid.New()

	uuid := binary.BigEndian.Uint64(u1[:8])
	return int64(uuid)
}

func FirstNonNil(datas ...interface{}) interface{} {
	for _, v := range datas {
		if v != nil {
			return v
		}
	}
	return nil
}

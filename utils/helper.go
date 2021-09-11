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

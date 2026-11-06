package uuid

import (
	"math/big"

	guuid "github.com/google/uuid"
)

func New() string {
	uuid := guuid.New()
	bigInt := new(big.Int)
	bigInt.SetBytes(uuid[:])

	// Convert to base36 (which uses 0-9 and a-z)
	return bigInt.Text(36)
}

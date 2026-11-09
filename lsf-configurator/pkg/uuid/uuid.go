// Package uuid generates short random IDs.
package uuid

import (
	"math/big"

	guuid "github.com/google/uuid"
)

// New returns a random UUID encoded in base 36, which is shorter than the usual form and contains only digits and
// lowercase letters.
func New() string {
	uuid := guuid.New()
	bigInt := new(big.Int)
	bigInt.SetBytes(uuid[:])

	// Convert to base36 (which uses 0-9 and a-z)
	return bigInt.Text(36)
}

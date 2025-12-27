package kekspose

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBootstrapAddress(t *testing.T) {
	k := Kekspose{}
	bootstrapAddress := k.bootstrapAddress(map[int32]uint32{0: 50000, 1: 50001, 2: 50002})

	assert.Equal(t, bootstrapAddress, "localhost:50000,localhost:50001,localhost:50002")
}

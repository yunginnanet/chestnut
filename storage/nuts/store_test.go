package nuts

import (
	"testing"

	"git.tcp.direct/kayos/chestnut-bitcask/storage/store_test"
)

func TestStore(t *testing.T) {
	store_test.TestStore(t, NewStore)
}

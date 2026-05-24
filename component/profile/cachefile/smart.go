package cachefile

import (
	"sync"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/component/smart"
	"github.com/metacubex/mihomo/log"
)

var (
	smartInitOnce  sync.Once
	smartStore     *smart.Store
)

func GetSmartStore() *smart.Store {
	smartInitOnce.Do(func() {
		c := Cache()
		if c == nil || c.DB == nil {
			smartStore = smart.NewStore(nil)
			return
		}

		err := c.DB.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketSmartStats)
			return err
		})

		if err != nil {
			log.Warnln("[CacheFile] write cache to %s failed: %s", c.DB.Path(), err.Error())
			smartStore = smart.NewStore(nil)
			return
		}
		smartStore = smart.NewStore(c.DB)
	})

	return smartStore
}
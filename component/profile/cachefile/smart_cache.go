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
	cache := Cache()
	if cache == nil || cache.DB == nil {
		log.Fatalln("[Smart] DB Cache file load failed")
	}

	smartInitOnce.Do(func() {
		err := cache.DB.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketSmartStats)
			return err
		})
		if err != nil {
			log.Fatalln("[SmartStore] Failed to create bucket: %v", err)
		}
		smartStore = smart.NewStore(cache.DB)
	})

	return smartStore
}
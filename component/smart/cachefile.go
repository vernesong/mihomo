package smart

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/common/batch"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/log"
)

// BatchSave 批量保存操作
func (s *Store) BatchSave(operations []StoreOperation) error {
	if len(operations) == 0 {
		return nil
	}

	concurrency := 2
	batchSize := 100

	var writeMapSync xsync.Map[string, []byte]

	b, _ := batch.New[struct{}](context.Background(), batch.WithConcurrencyNum[struct{}](concurrency))

	numBatches := (len(operations) + batchSize - 1) / batchSize

	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := (i + 1) * batchSize
		if end > len(operations) {
			end = len(operations)
		}

		curBatch := operations[start:end]
		b.Go(fmt.Sprintf("batch-%d", i), func() (struct{}, error) {
			for _, op := range curBatch {
				key := formatOperationKey(&op)
				if key != "" {
					writeMapSync.Store(key, op.Data)
				}
			}
			return struct{}{}, nil
		})
	}

	b.Wait()

	writeMap := make(map[string][]byte)
	writeMapSync.Range(func(key string, value []byte) bool {
		writeMap[key] = value
		return true
	})

	var err error
	err = db.Batch(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketSmartStats)
		if err != nil {
			return err
		}

		for key, data := range writeMap {
			if err := bucket.Put([]byte(key), data); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.Debugln("[SmartStore] Batch save operation failed: %v", err)
	}

	return err
}

// 刷新队列中的操作到数据库
func (s *Store) FlushQueue(force bool) {
	threshold := GetBatchSaveThreshold()
	ops := getGlobalQueueSnapshot()

	if len(ops) == 0 {
		return
	}

	if !force {
		if len(ops) < threshold {
			return
		}
	}

	InitQueue()
	s.BatchSave(ops)
	log.Debugln("[SmartStore] Queue datas saved, operations: [%d]", len(ops))
}

// 根据路径前缀获取所有匹配的数据
func (s *Store) GetSubBytesByPath(prefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	globalCacheParams.mutex.RLock()
	configMaxTargets := globalCacheParams.MaxTargets / 2
	globalCacheParams.mutex.RUnlock()

	pathParts := strings.Split(prefix, "/")
	if len(pathParts) < 2 || pathParts[0] != "smart" {
		return result, nil
	}

	keyType := pathParts[1]
	config := pathParts[2]
	group := ""
	if len(pathParts) >= 4 {
		group = pathParts[3]
	}

	strict := false
	switch keyType {
	case KeyTypeNode, KeyTypePrefetch, KeyTypeHostFailures:
		if len(pathParts) == 5 {
			strict = true
		}
	case KeyTypeRanking:
		if len(pathParts) == 4 {
			strict = true
		}
	case KeyTypeStats:
		if len(pathParts) == 6 {
			strict = true
		}
	}

	// 从队列获取结果
	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Config != config || op.Group != group {
			continue
		}
		var key string

		switch keyType {
		case KeyTypeNode:
			if op.Type == OpSaveNodeState && op.Node != "" {
				key = FormatDBKey(KeyTypeNode, op.Config, op.Group, op.Node)
				result[key] = op.Data
			}
		case KeyTypeStats:
			if op.Type == OpSaveStats && op.Target != "" && op.Node != "" {
				if len(pathParts) >= 5 && pathParts[4] != op.Target {
					continue
				}
				key = FormatDBKey(KeyTypeStats, op.Config, op.Group, op.Target, op.Node)
				result[key] = op.Data
			}
		case KeyTypePrefetch:
			if op.Type == OpSavePrefetch && op.Target != "" {
				if len(pathParts) >= 5 && pathParts[4] != op.Target {
					continue
				}
				key = FormatDBKey(KeyTypePrefetch, op.Config, op.Group, op.Target)
				result[key] = op.Data
			}
		case KeyTypeRanking:
			if op.Type == OpSaveRanking {
				key = FormatDBKey(KeyTypeRanking, op.Config, op.Group)
				result[key] = op.Data
			}
		case KeyTypeHostFailures:
			if op.Type == OpSaveHostFailures && op.Target != "" {
				if len(pathParts) >= 5 && pathParts[4] != op.Target {
					continue
				}
				key = FormatDBKey(KeyTypeHostFailures, op.Config, op.Group, op.Target)
				result[key] = op.Data
			}
		}
	}

	if strict && len(result) > 0 {
		return result, nil
	}

	maxResults := -1
	if configMaxTargets > 1 {
		maxResults = configMaxTargets
	}

	if cached, ok := dbResultCache.Get(prefix); ok && maxResults > 0 {
		for k, v := range cached {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	} else {
		dbResult, err := s.DBViewPrefixScan(prefix, maxResults, strict)
		if err != nil {
			return result, nil
		}
		// KeyTypeStats use other cache
		if maxResults > 0 && !(keyType == KeyTypeStats && strict) {
			dbResultCache.Set(prefix, dbResult)
		}
		for k, v := range dbResult {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}

	return result, nil
}

// 从数据库获取单个条目
func (s *Store) DBViewGetItem(key string) ([]byte, error) {
	var data []byte
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		value := bucket.Get([]byte(key))
		if value == nil {
			return errors.New("item not found")
		}

		data = make([]byte, len(value))
		copy(data, value)
		return nil
	})
	return data, err
}

// 将单个条目保存到数据库
func (s *Store) DBBatchPutItem(key string, value []byte) error {
	return db.Batch(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketSmartStats)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(key), value)
	})
}

// 扫描前缀匹配的记录并随机返回结果
func (s *Store) DBViewPrefixScan(prefix string, maxResults int, strict bool) (map[string][]byte, error) {
	result := make(map[string][]byte)

	if maxResults == 0 {
		return result, nil
	}

	type kv struct {
		key string
		val []byte
	}
	var kvs []kv

	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		prefixBytes := []byte(prefix)
		for k, v := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, v = cursor.Next() {
			if strict && len(k) > len(prefixBytes) && k[len(prefixBytes)] != '/' {
				continue
			}
			dataCopy := make([]byte, len(v))
			copy(dataCopy, v)
			kvs = append(kvs, kv{key: string(k), val: dataCopy})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if maxResults < 0 || len(kvs) <= maxResults {
		for _, item := range kvs {
			result[item.key] = item.val
		}
	} else {
		reservoir := kvs[:maxResults]
		for i := maxResults; i < len(kvs); i++ {
			j := rand.Intn(i + 1)
			if j < maxResults {
				reservoir[j] = kvs[i]
			}
		}
		for _, item := range reservoir {
			result[item.key] = item.val
		}
	}

	return result, nil
}

// 删除前缀匹配的所有记录
func (s *Store) DBBatchDeletePrefix(prefix string, strict bool) error {
	var keysToDelete [][]byte

	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		prefixBytes := []byte(prefix)

		for k, _ := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, _ = cursor.Next() {
			if strict && len(k) > len(prefixBytes) && k[len(prefixBytes)] != '/' {
				continue
			}
			keyBytes := make([]byte, len(k))
			copy(keyBytes, k)
			keysToDelete = append(keysToDelete, keyBytes)
		}
		return nil
	})

	if err != nil {
		return err
	}

	const batchSize = 200
	for i := 0; i < len(keysToDelete); i += batchSize {
		end := i + batchSize
		if end > len(keysToDelete) {
			end = len(keysToDelete)
		}

		batch := keysToDelete[i:end]
		err := db.Batch(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketSmartStats)
			if bucket == nil {
				return nil
			}

			for _, k := range batch {
				if err := bucket.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}

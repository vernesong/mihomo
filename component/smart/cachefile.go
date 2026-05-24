package smart

import (
	"bytes"
	"errors"
	"math/rand"
	"strings"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/log"
)

// BatchSave 批量保存操作
func (s *Store) BatchSave(operations []StoreOperation) error {
	if len(operations) == 0 {
		return nil
	}
	if db == nil {
		return errors.New("DB Cache file load failed")
	}

	type writeEntry struct {
		key  string
		data []byte
	}

	writeIndex := make(map[string]int, len(operations))
	entries := make([]writeEntry, 0, len(operations))
	for i := range operations {
		key := formatOperationKey(&operations[i])
		if key == "" {
			continue
		}
		if idx, ok := writeIndex[key]; ok {
			entries[idx].data = operations[i].Data
			continue
		}
		writeIndex[key] = len(entries)
		entries = append(entries, writeEntry{key: key, data: operations[i].Data})
	}
	if len(entries) == 0 {
		return nil
	}

	var err error
	err = db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			var err error
			bucket, err = tx.CreateBucketIfNotExists(bucketSmartStats)
			if err != nil {
				return err
			}
		}

		for _, entry := range entries {
			if err := bucket.Put([]byte(entry.key), entry.data); err != nil {
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
	ops := drainGlobalQueue(force)
	if len(ops) == 0 {
		return
	}
	s.BatchSave(ops)
	log.Debugln("[SmartStore] Queue datas saved, operations: [%d]", len(ops))
}

func matchKeyPrefix(key, queryPrefix string, strict bool) bool {
	if !strings.HasPrefix(key, queryPrefix) {
		return false
	}
	if strict && len(key) > len(queryPrefix) && key[len(queryPrefix)] != '/' {
		return false
	}
	return true
}

func mergeIntoByPrefix(from, into map[string][]byte, queryPrefix string, strict bool) int {
	merged := 0
	for k, v := range from {
		if !matchKeyPrefix(k, queryPrefix, strict) {
			continue
		}
		if _, exists := into[k]; exists {
			continue
		}
		into[k] = v
		merged++
	}
	return merged
}

// 根据路径获取缓存数据
func (s *Store) GetSubBytesByPath(prefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	globalCacheParams.mutex.RLock()
	configMaxTargets := globalCacheParams.MaxTargets / 2
	globalCacheParams.mutex.RUnlock()

	depth := strings.Count(prefix, "/") + 1
	if depth < 3 || !strings.HasPrefix(prefix, "smart/") {
		return result, nil
	}
	rest := prefix[6:] // skip "smart/"
	var keyType, config, group, seg4, seg5 string
	keyType, rest, _ = strings.Cut(rest, "/")
	config, rest, _ = strings.Cut(rest, "/")
	if depth >= 4 {
		group, rest, _ = strings.Cut(rest, "/")
	}
	if depth >= 5 {
		seg4, rest, _ = strings.Cut(rest, "/")
	}
	if depth >= 6 {
		seg5, _, _ = strings.Cut(rest, "/")
	}

	strict := false
	switch keyType {
	case KeyTypeNode, KeyTypePrefetch, KeyTypeHostFailures:
		if depth == 5 {
			strict = true
		}
	case KeyTypeRanking:
		if depth == 4 {
			strict = true
		}
	case KeyTypeStats:
		if depth == 6 {
			strict = true
		}
	}

	// 从队列获取结果
	ops := getGlobalQueueSnapshot()
	for _, op := range ops {
		if op.Config != config {
			continue
		}
		if depth >= 4 && op.Group != group {
			continue
		}

		switch keyType {
		case KeyTypeNode:
			if op.Type == OpSaveNodeState && op.Node != "" {
				if depth >= 5 && seg4 != op.Node {
					continue
				}
				result[FormatDBKey(KeyTypeNode, op.Config, op.Group, op.Node)] = op.Data
			}
		case KeyTypeStats:
			if op.Type == OpSaveStats && op.Target != "" && op.Node != "" {
				if depth >= 5 && seg4 != op.Target {
					continue
				}
				if depth >= 6 && seg5 != op.Node {
					continue
				}
				result[FormatDBKey(KeyTypeStats, op.Config, op.Group, op.Target, op.Node)] = op.Data
			}
		case KeyTypePrefetch:
			if op.Type == OpSavePrefetch && op.Target != "" {
				if depth >= 5 && seg4 != op.Target {
					continue
				}
				result[FormatDBKey(KeyTypePrefetch, op.Config, op.Group, op.Target)] = op.Data
			}
		case KeyTypeRanking:
			if op.Type == OpSaveRanking {
				result[FormatDBKey(KeyTypeRanking, op.Config, op.Group)] = op.Data
			}
		case KeyTypeHostFailures:
			if op.Type == OpSaveHostFailures && op.Target != "" {
				if depth >= 5 && seg4 != op.Target {
					continue
				}
				result[FormatDBKey(KeyTypeHostFailures, op.Config, op.Group, op.Target)] = op.Data
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

	hasGroupLevel := false
	var groupPrefix string

	switch keyType {
	case KeyTypeStats, KeyTypeNode, KeyTypePrefetch, KeyTypeHostFailures, KeyTypeRanking:
		if depth >= 4 {
			hasGroupLevel = true
			groupPrefix = FormatDBKey(keyType, config, group)
		}
	}

	if hasGroupLevel && maxResults > 0 {
		if cachedGroup, ok := dbResultCache.Get(groupPrefix); ok {
			merged := mergeIntoByPrefix(cachedGroup, result, prefix, strict)
			if depth == 4 || merged > 0 {
				return result, nil
			}
			if strict {
				groupResult, err := s.DBViewPrefixScan(groupPrefix, maxResults, false)
				if err == nil {
					dbResultCache.Set(groupPrefix, groupResult)
					merged = mergeIntoByPrefix(groupResult, result, prefix, strict)
					if depth == 4 || merged > 0 {
						return result, nil
					}
				}
			}
		} else {
			groupResult, err := s.DBViewPrefixScan(groupPrefix, maxResults, false)
			if err == nil {
				dbResultCache.Set(groupPrefix, groupResult)
				merged := mergeIntoByPrefix(groupResult, result, prefix, strict)
				if depth == 4 || merged > 0 {
					return result, nil
				}
			}
		}
		return result, nil
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
		if maxResults > 0 && !hasGroupLevel {
			if keyType != KeyTypeStats && keyType != KeyTypeHostFailures {
				dbResultCache.Set(prefix, dbResult)
			}
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
	if db == nil {
		return nil, errors.New("DB Cache file load failed")
	}
	keyBytes := []byte(key)

	var data []byte
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return errors.New("bucket not found")
		}

		value := bucket.Get(keyBytes)
		if value == nil {
			return errors.New("item not found")
		}

		data = append(data[:0], value...)
		return nil
	})
	return data, err
}

// 将单个条目保存到数据库
func (s *Store) DBBatchPutItem(key string, value []byte) error {
	if db == nil {
		return errors.New("DB Cache file load failed")
	}
	keyBytes := []byte(key)

	return db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			var err error
			bucket, err = tx.CreateBucketIfNotExists(bucketSmartStats)
			if err != nil {
				return err
			}
		}
		return bucket.Put(keyBytes, value)
	})
}

// 扫描前缀匹配的记录并随机返回结果
func (s *Store) DBViewPrefixScan(prefix string, maxResults int, strict bool) (map[string][]byte, error) {
	if db == nil {
		return nil, errors.New("DB Cache file load failed")
	}

	resultCap := 0
	if maxResults > 0 {
		resultCap = maxResults
	}
	result := make(map[string][]byte, resultCap)

	if maxResults == 0 {
		return result, nil
	}

	type kv struct {
		key string
		val []byte
	}

	var reservoir []kv
	seen := 0

	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		prefixBytes := []byte(prefix)

		if maxResults < 0 {
			for k, v := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, v = cursor.Next() {
				if strict && len(k) > len(prefixBytes) && k[len(prefixBytes)] != '/' {
					continue
				}
				keyCopy := string(k)
				dataCopy := make([]byte, len(v))
				copy(dataCopy, v)
				result[keyCopy] = dataCopy
			}
			return nil
		}

		reservoir = make([]kv, 0, maxResults)
		for k, v := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, v = cursor.Next() {
			if strict && len(k) > len(prefixBytes) && k[len(prefixBytes)] != '/' {
				continue
			}

			if len(reservoir) < maxResults {
				dataCopy := make([]byte, len(v))
				copy(dataCopy, v)
				reservoir = append(reservoir, kv{key: string(k), val: dataCopy})
				seen++
				continue
			}

			seen++
			j := rand.Intn(seen)
			if j < maxResults {
				dataCopy := make([]byte, len(v))
				copy(dataCopy, v)
				reservoir[j] = kv{key: string(k), val: dataCopy}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	for _, item := range reservoir {
		result[item.key] = item.val
	}

	return result, nil
}

// 删除前缀匹配的所有记录
func (s *Store) DBBatchDeletePrefix(prefixes []string, strict bool) error {
	if db == nil {
		return errors.New("DB Cache file load failed")
	}

	return db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		for _, prefix := range prefixes {
			prefixBytes := []byte(prefix)
			for k, _ := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); {
				if strict && len(k) > len(prefixBytes) && k[len(prefixBytes)] != '/' {
					k, _ = cursor.Next()
					continue
				}
				if err := cursor.Delete(); err != nil {
					return err
				}
				k, _ = cursor.Next()
			}
		}
		return nil
	})
}

package smart

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/common/batch"
	"github.com/metacubex/mihomo/common/singleflight"
	"github.com/metacubex/mihomo/common/xsync"
	"github.com/metacubex/mihomo/log"
)

var (
	opMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string][]byte, 64)
		},
	}

	opMapPoolStats = sync.Pool{
		New: func() interface{} {
			return make(map[string]*StoreOperation, 64)
		},
	}

	cacheUpdatePool = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{}, 64)
		},
	}
)

// BatchSave 批量保存操作
func (s *Store) BatchSave(operations []StoreOperation) error {
	if len(operations) == 0 {
		return nil
	}

	concurrency := 2
	batchSize := 100

	writeMap := opMapPool.Get().(map[string][]byte)

	defer func() {
		for k := range writeMap {
			delete(writeMap, k)
		}
		opMapPool.Put(writeMap)
	}()

	b, _ := batch.New[struct{}](context.Background(), batch.WithConcurrencyNum[struct{}](concurrency))
	var writeMapSync xsync.Map[string, []byte]

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
				var key string

				switch op.Type {
				case OpSaveNodeState:
					key = FormatDBKey("smart", KeyTypeNode, op.Config, op.Group, op.Node)
				case OpSaveStats:
					key = FormatDBKey("smart", KeyTypeStats, op.Config, op.Group, op.Target, op.Node)
				case OpSavePrefetch:
					key = FormatDBKey("smart", KeyTypePrefetch, op.Config, op.Group, op.Target)
				case OpSaveRanking:
					key = FormatDBKey("smart", KeyTypeRanking, op.Config, op.Group, "")
				}

				if key != "" && op.Data != nil {
					dataCopy := make([]byte, len(op.Data))
					copy(dataCopy, op.Data)
					writeMapSync.Store(key, dataCopy)
				}
			}
			return struct{}{}, nil
		})
	}

	b.Wait()

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

// 批量保存连接统计数据
func (s *Store) BatchSaveStats(operations []StoreOperation) error {
	if len(operations) == 0 {
		return nil
	}

	var cacheBatch xsync.Map[string, []byte]
	existingOps := getGlobalQueueSnapshot()
	existingOpsCopy := make([]StoreOperation, len(existingOps))
	copy(existingOpsCopy, existingOps)

	initialMapSize := len(existingOpsCopy) + len(operations)
	lookupToKeys := make(map[string][]string, initialMapSize/2)
	opMap := opMapPoolStats.Get().(map[string]*StoreOperation)
	defer func() {
		for k := range opMap {
			delete(opMap, k)
		}
		opMapPoolStats.Put(opMap)
	}()

	for i, op := range existingOpsCopy {
		var opKey string
		var lookupKey string

		if op.Type == OpSaveStats {
			lookupKey = fmt.Sprintf("%s:%s:%s:%s", op.Group, op.Config, op.Target, op.Node)
			opKey = fmt.Sprintf("%s:%d", lookupKey, i)
		} else {
			lookupKey = fmt.Sprintf("%d:%s:%s:%s:%s", op.Type, op.Group, op.Config, op.Target, op.Node)
			opKey = fmt.Sprintf("%s:%d", lookupKey, i)
		}

		opMap[opKey] = &existingOpsCopy[i]
	}

	for opKey, op := range opMap {
		var lookupKey string
		if op.Type == OpSaveStats {
			lookupKey = fmt.Sprintf("%s:%s:%s:%s", op.Group, op.Config, op.Target, op.Node)
		} else {
			lookupKey = fmt.Sprintf("%d:%s:%s:%s:%s", op.Type, op.Group, op.Config, op.Target, op.Node)
		}
		lookupToKeys[lookupKey] = append(lookupToKeys[lookupKey], opKey)
	}

	concurrency := 2
	batchSize := 100

	b, _ := batch.New[struct{}](context.Background(), batch.WithConcurrencyNum[struct{}](concurrency))
	processGroup := singleflight.Group[struct{}]{}

	for batchStart := 0; batchStart < len(operations); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(operations) {
			batchEnd = len(operations)
		}

		batchIndex := batchStart
		b.Go(fmt.Sprintf("batch-%d", batchIndex/batchSize), func() (struct{}, error) {
			start, end := batchIndex, batchEnd
			for i := start; i < end; i++ {
				op := operations[i]
				var lookupKey string

				if op.Type == OpSaveStats {
					lookupKey = fmt.Sprintf("%s:%s:%s:%s", op.Group, op.Config, op.Target, op.Node)

					processGroup.Do(lookupKey, func() (struct{}, error) {
						matchingKeys, found := lookupToKeys[lookupKey]
						if !found || len(matchingKeys) == 0 {
							newKey := fmt.Sprintf("%s:%d", lookupKey, len(opMap))
							opMap[newKey] = &op
							lookupToKeys[lookupKey] = append(lookupToKeys[lookupKey], newKey)

							if op.Data != nil {
								cacheKey := FormatCacheKey(KeyTypeStats, op.Config, op.Group, op.Target, op.Node)
								dataCopy := make([]byte, len(op.Data))
								copy(dataCopy, op.Data)
								cacheBatch.Store(cacheKey, dataCopy)
							}
							return struct{}{}, nil
						}

						existingOp := opMap[matchingKeys[0]]
						var existingRecord, newRecord StatsRecord

						if existingOp.Data != nil && op.Data != nil &&
							json.Unmarshal(existingOp.Data, &existingRecord) == nil &&
							json.Unmarshal(op.Data, &newRecord) == nil {

							oldWeights := make(map[string]float64, len(existingRecord.Weights))
							if existingRecord.Weights != nil {
								for k, v := range existingRecord.Weights {
									oldWeights[k] = v
								}
							}

							existingRecord = newRecord

							if existingRecord.Success > 1000000 {
								existingRecord.Success = existingRecord.Success / 2
							}
							if existingRecord.Failure > 1000000 {
								existingRecord.Failure = existingRecord.Failure / 2
							}

							if len(oldWeights) > 0 {
								if existingRecord.Weights == nil {
									existingRecord.Weights = oldWeights
								} else {
									for k, v := range oldWeights {
										if _, exists := existingRecord.Weights[k]; !exists {
											existingRecord.Weights[k] = v
										}
									}
								}
							}

							mergedData, err := json.Marshal(existingRecord)
							if err == nil {
								existingOp.Data = mergedData

								cacheKey := FormatCacheKey(KeyTypeStats, op.Config, op.Group, op.Target, op.Node)
								cacheBatch.Store(cacheKey, mergedData)
							}
						}

						return struct{}{}, nil
					})
				} else {
					lookupKey = fmt.Sprintf("%d:%s:%s:%s:%s", op.Type, op.Group, op.Config, op.Target, op.Node)

					newKey := fmt.Sprintf("%s:%d", lookupKey, len(opMap))
					opMap[newKey] = &op
					lookupToKeys[lookupKey] = append(lookupToKeys[lookupKey], newKey)

					if op.Data != nil {
						var cacheKey string
						switch op.Type {
						case OpSaveNodeState:
							cacheKey = FormatCacheKey(KeyTypeNode, op.Config, op.Group, op.Node)
							dataCopy := make([]byte, len(op.Data))
							copy(dataCopy, op.Data)
							cacheBatch.Store(cacheKey, dataCopy)
						case OpSavePrefetch:
							cacheKey = FormatCacheKey(KeyTypePrefetch, op.Config, op.Group, op.Target)
							dataCopy := make([]byte, len(op.Data))
							copy(dataCopy, op.Data)
							cacheBatch.Store(cacheKey, dataCopy)
						case OpSaveRanking:
							cacheKey = FormatCacheKey(KeyTypeRanking, op.Config, op.Group, "")
							dataCopy := make([]byte, len(op.Data))
							copy(dataCopy, op.Data)
							cacheBatch.Store(cacheKey, dataCopy)
						}
					}
				}
			}
			return struct{}{}, nil
		})
	}

	b.Wait()
	processGroup.Reset()

	newQueue := make([]StoreOperation, 0, len(opMap))
	for _, op := range opMap {
		newQueue = append(newQueue, *op)
	}

	replaceGlobalQueue(newQueue)

	currentThreshold := GetBatchSaveThreshold()

	needFlush := len(newQueue) >= currentThreshold

	cacheUpdates := cacheUpdatePool.Get().(map[string]interface{})
	defer func() {
		for k := range cacheUpdates {
			delete(cacheUpdates, k)
		}
		cacheUpdatePool.Put(cacheUpdates)
	}()

	cacheBatch.Range(func(key string, value []byte) bool {
		cacheUpdates[key] = value
		return true
	})

	if len(cacheUpdates) > 0 {
		for key, value := range cacheUpdates {
			SetCacheValue(key, value)
		}
	}

	if needFlush {
		go s.FlushQueue(needFlush)
	}

	return nil
}

// 刷新队列中的操作到数据库
func (s *Store) FlushQueue(isThresholdTriggered bool) {
	threshold := MinBatchThreshLimit
	if globalCacheParams.BatchSaveThreshold > 0 {
		threshold = GetBatchSaveThreshold()
	}

	emptyQueue := make([]StoreOperation, 0, threshold)
	ops := swapGlobalQueue(emptyQueue)

	if len(ops) == 0 {
		return
	}

	if len(ops) <= 100 {
		s.BatchSave(ops)
		log.Debugln("[SmartStore] Queue datas saved, operations: [%d]", len(ops))
		return
	}

	maxBatchSize := 100
	totalOps := len(ops)
	batchCount := (totalOps + maxBatchSize - 1) / maxBatchSize
	concurrency := 2

	b, _ := batch.New[int](context.Background(), batch.WithConcurrencyNum[int](concurrency))
	opsBatchPool := sync.Pool{
		New: func() interface{} {
			return make([]StoreOperation, 0, maxBatchSize)
		},
	}

	for i := 0; i < batchCount; i++ {
		batchIndex := i
		b.Go(fmt.Sprintf("batch-%d", i), func() (int, error) {
			startIdx := batchIndex * maxBatchSize
			endIdx := (batchIndex + 1) * maxBatchSize
			if endIdx > totalOps {
				endIdx = totalOps
			}

			batchOps := opsBatchPool.Get().([]StoreOperation)
			batchOps = batchOps[:0]
			batchOps = append(batchOps, ops[startIdx:endIdx]...)

			s.BatchSave(batchOps)

			for idx := range batchOps {
				batchOps[idx] = StoreOperation{}
			}
			opsBatchPool.Put(batchOps)

			return 0, nil
		})
	}

	b.Wait()

	for i := range ops {
		ops[i] = StoreOperation{}
	}
	ops = nil

	log.Debugln("[SmartStore] Queue datas saved, operations: [%d]", totalOps)
}

// 根据路径前缀获取所有匹配的数据
func (s *Store) GetSubBytesByPath(prefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	globalCacheParams.mutex.RLock()
	configMaxTargets := globalCacheParams.MaxTargets
	globalCacheParams.mutex.RUnlock()

	pathParts := strings.Split(prefix, "/")
	if len(pathParts) < 3 || pathParts[0] != "smart" {
		return result, nil
	}

	keyType := pathParts[1]
	config := pathParts[2]
	group := ""
	if len(pathParts) >= 4 {
		group = pathParts[3]
	}

	switch keyType {
	case KeyTypeNode, KeyTypePrefetch:
		if len(pathParts) == 5 && pathParts[4] != "" {
			configMaxTargets = 1
		}
	case KeyTypeRanking:
		if len(pathParts) == 5 && pathParts[3] != "" {
			configMaxTargets = 1
		}
	case KeyTypeStats:
		if len(pathParts) == 6 && pathParts[5] != "" {
			configMaxTargets = 1
		}
	}

	cacheLookup := func(cacheKey string) (dbKey string, dataBytes []byte, ok bool) {
		value, ok := GetCacheValue(cacheKey)
		if !ok {
			return "", nil, false
		}
		switch v := value.(type) {
		case []byte:
			if len(v) == 0 {
				return "", nil, false
			}
			dataBytes = make([]byte, len(v))
			copy(dataBytes, v)
		default:
			b, err := json.Marshal(v)
			if err != nil || len(b) == 0 {
				return "", nil, false
			}
			dataBytes = b
		}
		parts := strings.Split(cacheKey, ":")
		var cacheGroup string
		if len(parts) >= 3 {
			cacheGroup = parts[2]
		} else {
			return "", nil, false
		}
		if keyType == KeyTypeStats {
			if len(parts) < 5 {
				return "", nil, false
			}
			node := parts[len(parts)-1]
			target := strings.Join(parts[3:len(parts)-1], ":")
			dbKey = FormatDBKey("smart", keyType, config, cacheGroup, target, node)
		} else if len(parts) >= 4 {
			target := strings.Join(parts[3:], ":")
			dbKey = FormatDBKey("smart", keyType, config, cacheGroup, target)
		} else {
			return "", nil, false
		}
		return dbKey, dataBytes, true
	}

	if configMaxTargets == 1 {
		cacheKey := strings.Join(pathParts[1:], ":")
		dbKey, dataBytes, ok := cacheLookup(cacheKey)
		if ok {
			result[dbKey] = dataBytes
			return result, nil
		}
	} else {
		cachePrefix := FormatCacheKey(keyType, config, group)
		cacheResults := GetCacheValuesByPrefix(cachePrefix)
		if len(cacheResults) > 0 {
			keys := make([]string, 0, len(cacheResults))
			for key := range cacheResults {
				keys = append(keys, key)
			}
			rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

			for _, key := range keys {
				if len(result) >= configMaxTargets {
					break
				}
				dbKey, dataBytes, ok := cacheLookup(key)
				if !ok {
					continue
				}
				result[dbKey] = dataBytes
			}

			if len(result) >= configMaxTargets {
				return result, nil
			}
		}
	}

	dbCount, err := s.DBViewPrefixCount(prefix)
	if err != nil {
		return nil, err
	}
	if dbCount == 0 {
		return result, nil
	}

	warmThreshold := (dbCount*80 + 99) / 100
	if len(result) >= warmThreshold || dbCount <= len(result) {
		return result, nil
	}

	remaining := configMaxTargets - len(result)
	if remaining <= 0 {
		return result, nil
	}

	dbResult, err := s.DBViewPrefixScan(prefix, remaining)
	if err != nil {
		return nil, err
	}

	for fullPath, data := range dbResult {
		if _, exists := result[fullPath]; exists {
			continue
		}
		UpdateCacheFromDBResult(fullPath, data)
		result[fullPath] = data
		if len(result) >= configMaxTargets {
			break
		}
	}

	return result, nil
}

// 删除指定路径前缀的数据
func (s *Store) DeleteByPath(path string) error {
	return s.DBBatchDeletePrefix(path)
}

// 从数据库结果更新缓存
func UpdateCacheFromDBResult(fullPath string, data []byte) {
	if data == nil || len(data) == 0 || fullPath == "" {
		return
	}

	cacheKey := ExtractCachePrefixFromPath(fullPath)
	if cacheKey == "" {
		return
	}

	SetCacheValue(cacheKey, data)
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

// 计算前缀匹配的记录数量
func (s *Store) DBViewPrefixCount(prefix string) (int, error) {
	var count int
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		prefixBytes := []byte(prefix)

		for k, _ := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, _ = cursor.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

// 扫描前缀匹配的记录并随机返回结果
func (s *Store) DBViewPrefixScan(prefix string, maxResults int) (map[string][]byte, error) {
	result := make(map[string][]byte)

	if maxResults == 0 {
		return result, nil
	}

	if maxResults < 0 {
		err := db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketSmartStats)
			if bucket == nil {
				return nil
			}
			cursor := bucket.Cursor()
			prefixBytes := []byte(prefix)
			for k, v := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, v = cursor.Next() {
				dataCopy := make([]byte, len(v))
				copy(dataCopy, v)
				result[string(k)] = dataCopy
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	type kv struct {
		key string
		val []byte
	}
	reservoir := make([]kv, 0, maxResults)
	total := 0

	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		prefixBytes := []byte(prefix)
		for k, v := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, v = cursor.Next() {
			total++
			dataCopy := make([]byte, len(v))
			copy(dataCopy, v)
			item := kv{key: string(k), val: dataCopy}

			if len(reservoir) < maxResults {
				reservoir = append(reservoir, item)
			} else {
				j := rand.Intn(total)
				if j < maxResults {
					reservoir[j] = item
				}
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
func (s *Store) DBBatchDeletePrefix(prefix string) error {
	var keysToDelete [][]byte

	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketSmartStats)
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		prefixBytes := []byte(prefix)

		for k, _ := cursor.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, _ = cursor.Next() {
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

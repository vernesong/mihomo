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
	"time"

	"github.com/metacubex/bbolt"
	"github.com/metacubex/mihomo/common/batch"
	"github.com/metacubex/mihomo/common/singleflight"
	"github.com/metacubex/mihomo/log"
)

var (
	opMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string][]byte, 64)
		},
	}

	cacheUpdatePool = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{}, 64)
		},
	}
)

type Store struct {
	db *bbolt.DB

	networkFailureManager struct {
		status        map[string]bool
		successCount  map[string]int
		lastFailure   map[string]time.Time
		lock          sync.RWMutex
		cacheThrottle struct {
			mutex     sync.Mutex
			lastSet   map[string]time.Time
			lastClear map[string]time.Time
		}
		writeInterval time.Duration
		clearInterval time.Duration
	}
}

func NewStore(db *bbolt.DB) *Store {
	s := &Store{
		db: db,
	}
	s.networkFailureManager.status = make(map[string]bool)
	s.networkFailureManager.successCount = make(map[string]int)
	s.networkFailureManager.lastFailure = make(map[string]time.Time)
	s.networkFailureManager.writeInterval = 5 * time.Second
	s.networkFailureManager.clearInterval = 5 * time.Second
	s.networkFailureManager.cacheThrottle.lastSet = make(map[string]time.Time)
	s.networkFailureManager.cacheThrottle.lastClear = make(map[string]time.Time)

	globalQueueMutex.Lock()
	if globalOperationQueue == nil {
		threshold := GetBatchSaveThreshold()
		globalOperationQueue = make([]StoreOperation, 0, threshold)
	}
	globalQueueMutex.Unlock()

	return s
}

// BatchSave 批量保存操作
func (s *Store) BatchSave(operations []StoreOperation) error {
	if len(operations) == 0 {
		return nil
	}

	concurrency := 2
	batchSize := 100

	writeMap := opMapPool.Get().(map[string][]byte)
	cacheUpdates := cacheUpdatePool.Get().(map[string]interface{})

	defer func() {
		for k := range writeMap {
			delete(writeMap, k)
		}
		opMapPool.Put(writeMap)
		for k := range cacheUpdates {
			delete(cacheUpdates, k)
		}
		cacheUpdatePool.Put(cacheUpdates)
	}()

	b, _ := batch.New[struct{}](context.Background(), batch.WithConcurrencyNum[struct{}](concurrency))
	var writeMapSync sync.Map
	var cacheUpdatesSync sync.Map

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
					key = FormatDBKey("smart", KeyTypeStats, op.Config, op.Group, op.Domain, op.Node)
				case OpSavePrefetch:
					key = FormatDBKey("smart", KeyTypePrefetch, op.Config, op.Group, op.Domain)
				case OpSaveRanking:
					key = FormatDBKey("smart", KeyTypeRanking, op.Config, op.Group, "")
				}

				if key != "" && op.Data != nil {
					dataCopy := make([]byte, len(op.Data))
					copy(dataCopy, op.Data)
					writeMapSync.Store(key, dataCopy)

					var cacheKey string
					switch op.Type {
					case OpSaveNodeState:
						cacheKey = FormatCacheKey(KeyTypeNode, op.Config, op.Group, op.Node)
						var nodeState NodeState
						if json.Unmarshal(op.Data, &nodeState) == nil {
							cacheUpdatesSync.Store(cacheKey, nodeState)
						}
					case OpSaveStats:
						cacheKey = FormatCacheKey(KeyTypeStats, op.Config, op.Group, op.Domain, op.Node)
						var record StatsRecord
						if json.Unmarshal(op.Data, &record) == nil {
							cacheUpdatesSync.Store(cacheKey, record)
						}
					case OpSavePrefetch:
						cacheKey = FormatCacheKey(KeyTypePrefetch, op.Config, op.Group, op.Domain)
						var prefetchMap map[string]interface{}
						if json.Unmarshal(op.Data, &prefetchMap) == nil {
							cacheUpdatesSync.Store(cacheKey, prefetchMap)
						}
					case OpSaveRanking:
						cacheKey = FormatCacheKey(KeyTypeRanking, op.Config, op.Group, "")
						var rankingData RankingData
						if json.Unmarshal(op.Data, &rankingData) == nil {
							cacheUpdatesSync.Store(cacheKey, rankingData)
						}
					}
				}
			}
			return struct{}{}, nil
		})
	}

	b.Wait()

	writeMapSync.Range(func(key, value interface{}) bool {
		writeMap[key.(string)] = value.([]byte)
		return true
	})

	cacheUpdatesSync.Range(func(key, value interface{}) bool {
		cacheUpdates[key.(string)] = value
		return true
	})

	var err error
	err = s.db.Batch(func(tx *bbolt.Tx) error {
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

	if len(cacheUpdates) > 0 {
		for key, value := range cacheUpdates {
			SetCacheValue(key, value)
		}
	}

	if err != nil {
		log.Debugln("[SmartStore] Batch save operation failed: %v", err)
	}

	return err
}

// 批量保存连接统计数据
func (s *Store) BatchSaveConnStats(operations []StoreOperation) error {
	if len(operations) == 0 {
		return nil
	}

	globalQueueMutex.RLock()
	existingOps := make([]StoreOperation, len(globalOperationQueue))
	copy(existingOps, globalOperationQueue)
	globalQueueMutex.RUnlock()

	initialMapSize := len(existingOps) + len(operations)
	opMap := make(map[string]*StoreOperation, initialMapSize)
	lookupToKeys := make(map[string][]string, initialMapSize/2)
	cacheBatch := sync.Map{}

	for i, op := range existingOps {
		var opKey string
		var lookupKey string

		if op.Type == OpSaveStats {
			lookupKey = fmt.Sprintf("%s:%s:%s:%s", op.Group, op.Config, op.Domain, op.Node)
			opKey = fmt.Sprintf("%s:%d", lookupKey, i)
		} else {
			lookupKey = fmt.Sprintf("%d:%s:%s:%s:%s", op.Type, op.Group, op.Config, op.Domain, op.Node)
			opKey = fmt.Sprintf("%s:%d", lookupKey, i)
		}

		opMap[opKey] = &existingOps[i]
	}

	for opKey, op := range opMap {
		var lookupKey string
		if op.Type == OpSaveStats {
			lookupKey = fmt.Sprintf("%s:%s:%s:%s", op.Group, op.Config, op.Domain, op.Node)
		} else {
			lookupKey = fmt.Sprintf("%d:%s:%s:%s:%s", op.Type, op.Group, op.Config, op.Domain, op.Node)
		}
		lookupToKeys[lookupKey] = append(lookupToKeys[lookupKey], opKey)
	}

	concurrency := 2
	batchSize := 100

	b, _ := batch.New[StoreOperation](context.Background(), batch.WithConcurrencyNum[StoreOperation](concurrency))
	processGroup := singleflight.Group[StoreOperation]{}

	for batchStart := 0; batchStart < len(operations); batchStart += batchSize {
		batchEnd := batchStart + batchSize

		if batchEnd > len(operations) {
			batchEnd = len(operations)
		}

		batchIndex := batchStart
		b.Go(fmt.Sprintf("batch-%d", batchIndex/batchSize), func() (StoreOperation, error) {
			start, end := batchIndex, batchEnd
			for i := start; i < end; i++ {
				op := operations[i]
				var lookupKey string

				if op.Type == OpSaveStats {
					lookupKey = fmt.Sprintf("%s:%s:%s:%s", op.Group, op.Config, op.Domain, op.Node)

					processGroup.Do(lookupKey, func() (StoreOperation, error) {
						matchingKeys, found := lookupToKeys[lookupKey]
						if !found || len(matchingKeys) == 0 {
							newKey := fmt.Sprintf("%s:%d", lookupKey, len(opMap))
							opMap[newKey] = &op
							lookupToKeys[lookupKey] = append(lookupToKeys[lookupKey], newKey)

							if op.Data != nil {
								cacheKey := FormatCacheKey(KeyTypeStats, op.Config, op.Group, op.Domain, op.Node)
								var record StatsRecord
								if json.Unmarshal(op.Data, &record) == nil {
									cacheBatch.Store(cacheKey, &record)
								}
							}
							return op, nil
						}

						existingOp := opMap[matchingKeys[0]]
						var existingRecord, newRecord StatsRecord

						if json.Unmarshal(existingOp.Data, &existingRecord) == nil &&
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

								cacheKey := FormatCacheKey(KeyTypeStats, op.Config, op.Group, op.Domain, op.Node)
								cacheBatch.Store(cacheKey, &existingRecord)
							}
						}

						return *existingOp, nil
					})
				} else {
					lookupKey = fmt.Sprintf("%d:%s:%s:%s:%s", op.Type, op.Group, op.Config, op.Domain, op.Node)

					newKey := fmt.Sprintf("%s:%d", lookupKey, len(opMap))
					opMap[newKey] = &op
					lookupToKeys[lookupKey] = append(lookupToKeys[lookupKey], newKey)

					if op.Data != nil {
						var cacheKey string
						switch op.Type {
						case OpSaveNodeState:
							cacheKey = FormatCacheKey(KeyTypeNode, op.Config, op.Group, op.Node)
							var nodeState NodeState
							if json.Unmarshal(op.Data, &nodeState) == nil {
								cacheBatch.Store(cacheKey, nodeState)
							}
						case OpSavePrefetch:
							cacheKey = FormatCacheKey(KeyTypePrefetch, op.Config, op.Group, op.Domain)
							var prefetchMap map[string]interface{}
							if json.Unmarshal(op.Data, &prefetchMap) == nil {
								cacheBatch.Store(cacheKey, prefetchMap)
							}
						case OpSaveRanking:
							cacheKey = FormatCacheKey(KeyTypeRanking, op.Config, op.Group, "")
							var rankingData RankingData
							if json.Unmarshal(op.Data, &rankingData) == nil {
								cacheBatch.Store(cacheKey, rankingData)
							}
						}
					}
				}
			}
			return StoreOperation{}, nil
		})
	}

	b.Wait()
	processGroup.Reset()

	newQueue := make([]StoreOperation, 0, len(opMap))
	for _, op := range opMap {
		newQueue = append(newQueue, *op)
	}

	globalQueueMutex.Lock()
	globalOperationQueue = newQueue

	globalCacheParams.mutex.RLock()
	currentThreshold := globalCacheParams.BatchSaveThreshold
	globalCacheParams.mutex.RUnlock()

	needFlush := len(globalOperationQueue) >= currentThreshold
	globalQueueMutex.Unlock()

	cacheUpdates := make(map[string]interface{}, len(opMap)/2)
	cacheBatch.Range(func(key, value interface{}) bool {
		cacheUpdates[key.(string)] = value
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
	globalQueueMutex.Lock()
	if len(globalOperationQueue) == 0 {
		globalQueueMutex.Unlock()
		return
	}

	threshold := MinBatchThreshLimit
	globalCacheParams.mutex.RLock()
	if globalCacheParams.BatchSaveThreshold > 0 {
		threshold = globalCacheParams.BatchSaveThreshold
	}
	globalCacheParams.mutex.RUnlock()

	if len(globalOperationQueue) <= 100 {
		ops := globalOperationQueue
		globalOperationQueue = make([]StoreOperation, 0, threshold)
		globalQueueMutex.Unlock()

		s.BatchSave(ops)
		log.Debugln("[SmartStore] Queue datas saved, operations: [%d]", len(ops))
		return
	}

	ops := globalOperationQueue
	globalOperationQueue = make([]StoreOperation, 0, threshold)
	globalQueueMutex.Unlock()

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
func (s *Store) GetSubBytesByPath(prefix string, all bool) (map[string][]byte, error) {
	result := make(map[string][]byte)

	globalCacheParams.mutex.RLock()
	configMaxDomains := globalCacheParams.MaxDomains
	globalCacheParams.mutex.RUnlock()

	maxDomainsLimit := 500
	if all {
		maxDomainsLimit = configMaxDomains
	} else {
		if configMaxDomains < 500 {
			maxDomainsLimit = configMaxDomains
		}
	}

	var cachePrefix string
	pathParts := strings.Split(prefix, "/")
	if len(pathParts) >= 3 && pathParts[0] == "smart" {
		keyType := pathParts[1]
		config := pathParts[2]
		group := ""
		if len(pathParts) >= 4 {
			group = pathParts[3]
		}

		cachePrefix = FormatCacheKey(keyType, config, group)

		cacheResults := GetCacheValuesByPrefix(cachePrefix)

		if len(cacheResults) > int(float64(maxDomainsLimit)*0.6) && rand.Float64() > 0.15 {
			recordCount := 0

			keys := make([]string, 0, len(cacheResults))
			for key := range cacheResults {
				keys = append(keys, key)
			}

			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			for _, key := range keys {
				if recordCount >= maxDomainsLimit {
					break
				}
				recordCount++

				value := cacheResults[key]
				var data []byte
				var err error

				switch v := value.(type) {
				case []byte:
					data = make([]byte, len(v))
					copy(data, v)
				case StatsRecord:
					data, err = json.Marshal(v)
				case NodeState:
					data, err = json.Marshal(v)
				case map[string]string:
					data, err = json.Marshal(v)
				case RankingData:
					data, err = json.Marshal(v)
				default:
					continue
				}

				if err == nil && data != nil {
					parts := strings.Split(key, ":")
					var dbKey string

					if len(parts) >= 5 && keyType == KeyTypeStats {
						dbKey = FormatDBKey("smart", keyType, config, group, parts[3], parts[4])
					} else if len(parts) >= 4 && keyType != KeyTypeStats {
						dbKey = FormatDBKey("smart", keyType, config, group, parts[3])
					} else {
						continue
					}

					result[dbKey] = data
				}
			}

			if len(result) > 0 {
				return result, nil
			}
		}
	}

	dbResult, err := s.DBViewPrefixScan(prefix, maxDomainsLimit)
	if err != nil {
		return nil, err
	}

	for fullPath, data := range dbResult {
		UpdateCacheFromDBResult(fullPath, data)
	}

	return dbResult, nil
}

// 删除指定路径前缀的数据
func (s *Store) DeleteByPath(path string) error {
	keysToDelete := []string{}

	matchingData, err := s.DBViewPrefixScan(path, 10000)
	if err != nil {
		return err
	}

	for pathStr := range matchingData {
		cacheKey := ExtractCachePrefixFromPath(pathStr)
		if cacheKey != "" {
			keysToDelete = append(keysToDelete, cacheKey)
		}
	}

	err = s.DBBatchDeletePrefix(path)

	if err == nil && len(keysToDelete) > 0 {
		for _, key := range keysToDelete {
			DeleteCacheValue(key)
		}
	}

	return err
}

// 从数据库获取单个条目
func (s *Store) DBViewGetItem(key string) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
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
	return s.db.Batch(func(tx *bbolt.Tx) error {
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
	err := s.db.View(func(tx *bbolt.Tx) error {
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
	return count, err
}

// 扫描前缀匹配的记录并随机返回结果
func (s *Store) DBViewPrefixScan(prefix string, maxResults int) (map[string][]byte, error) {
	result := make(map[string][]byte)

	count, err := s.DBViewPrefixCount(prefix)
	if err != nil {
		return nil, err
	}

	if count <= maxResults {
		err := s.db.View(func(tx *bbolt.Tx) error {
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
		return result, err
	}

	type kv struct {
		key string
		val []byte
	}
	reservoir := make([]kv, 0, maxResults)
	total := 0

	err = s.db.View(func(tx *bbolt.Tx) error {
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

	for _, item := range reservoir {
		result[item.key] = item.val
	}
	return result, err
}

// 删除前缀匹配的所有记录
func (s *Store) DBBatchDeletePrefix(prefix string) error {
	var keysToDelete [][]byte

	err := s.db.View(func(tx *bbolt.Tx) error {
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
		err := s.db.Batch(func(tx *bbolt.Tx) error {
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

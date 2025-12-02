package updater

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/resource"
	"github.com/metacubex/mihomo/component/smart/lightgbm"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"

	"github.com/vernesong/leaves"
)

var (
	lgbmAutoUpdate     bool
	lgbmUpdateInterval int
	lgbmUrl            string

	updatingLgbm atomic.Bool
)

func LgbmAutoUpdate() bool {
	return lgbmAutoUpdate
}

func LgbmUpdateInterval() int {
	return lgbmUpdateInterval
}

func LgbmUrl() string {
	return lgbmUrl
}

func SetLgbmAutoUpdate(newAutoUpdate bool) {
	lgbmAutoUpdate = newAutoUpdate
}

func SetLgbmUpdateInterval(newUpdateInterval int) {
	lgbmUpdateInterval = newUpdateInterval
}

func SetLgbmUrl(newUrl string) {
	lgbmUrl = newUrl
}

func UpdateLgbmModel() (err error) {
	modelUrl := lgbmUrl
	if modelUrl == "" {
		modelUrl = lightgbm.GetModelDownloadURL()
	}

	vehicle := resource.NewHTTPVehicle(modelUrl, C.Path.SmartModel(), "", nil, defaultHttpTimeout, 0)
	var oldHash utils.HashType
	if buf, err := os.ReadFile(vehicle.Path()); err == nil {
		oldHash = utils.MakeHash(buf)
	}
	data, hash, err := vehicle.Read(context.Background(), oldHash)
	if err != nil {
		return fmt.Errorf("can't download LightGBM model file: %w", err)
	}
	if oldHash.Equal(hash) {
		log.Infoln("[Smart] LightGBM model is up to date")
		return nil
	}
	if len(data) == 0 {
		return fmt.Errorf("can't download LightGBM model file: no data")
	}

	tmpFile, err := os.CreateTemp("", "lgbm_model_*.bin")
	if err != nil {
		return fmt.Errorf("failed to create temp file for validation: %w", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	_, err = leaves.LGEnsembleFromFile(tmpPath, false)
	if err != nil {
		return fmt.Errorf("invalid LightGBM model file: %s", err)
	}

	if err = vehicle.Write(data); err != nil {
		return fmt.Errorf("can't save LightGBM model file: %w", err)
	}

	defer func() {
		log.Infoln("[Smart] LightGBM model update completed")
		lightgbm.ReloadModel()
	}()

	return nil
}

func updateLgbmModel() error {
	defer runtime.GC()

	return UpdateLgbmModel()
}

var ErrGetLgbmModelUpdateSkip = errors.New("LightGBM model is updating, skip")

func UpdateLgbmModelDatabase() error {
	if updatingLgbm.Load() {
		return ErrGetLgbmModelUpdateSkip
	}

	log.Infoln("[Smart] Start updating LightGBM model")

	updatingLgbm.Store(true)
	defer updatingLgbm.Store(false)

	if err := updateLgbmModel(); err != nil {
		log.Errorln("[Smart] update LightGBM model error: %s", err.Error())
		return err
	}

	return nil
}

func getLgbmModelUpdateTime() (time time.Time, err error) {
	fileInfo, err := os.Stat(C.Path.SmartModel())
	if err == nil {
		return fileInfo.ModTime(), nil
	}
	return
}

func RegisterLgbmUpdater() {
	if lgbmUpdateInterval <= 0 {
		log.Errorln("[Smart] Invalid update interval: %d", lgbmUpdateInterval)
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(lgbmUpdateInterval) * time.Hour)
		defer ticker.Stop()

		lastUpdate, err := getLgbmModelUpdateTime()
		if err != nil {
			log.Errorln("[Smart] Get LightGBM model update time error: %s", err.Error())
			return
		}

		log.Infoln("[Smart] last update time %s", lastUpdate)
		if lastUpdate.Add(time.Duration(lgbmUpdateInterval) * time.Hour).Before(time.Now()) {
			log.Infoln("[Smart] Model has not been updated for %v, update now", time.Duration(lgbmUpdateInterval)*time.Hour)
			if err := UpdateLgbmModelDatabase(); err != nil {
				log.Errorln("[Smart] Failed to update LightGBM model: %s", err.Error())
				return
			}
		}

		for range ticker.C {
			log.Infoln("[Smart] updating model every %d hours", lgbmUpdateInterval)
			if err := UpdateLgbmModelDatabase(); err != nil {
				log.Errorln("[Smart] Failed to update LightGBM model: %s", err.Error())
			}
		}
	}()
}

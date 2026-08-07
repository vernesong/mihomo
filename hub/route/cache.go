package route

import (
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func cacheRouter() http.Handler {
	r := chi.NewRouter()
	r.Post("/fakeip/flush", flushFakeIPPool)
	r.Post("/smart/flush", flushAllSmartCache)
	r.Post("/smart/flush/{config}", flushSmartConfigCache)
	r.Post("/dns/flush", flushDnsCache)
	return r
}

func flushFakeIPPool(w http.ResponseWriter, r *http.Request) {
	err := resolver.FlushFakeIP()
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	render.NoContent(w, r)
}

func flushAllSmartCache(w http.ResponseWriter, r *http.Request) {
	smartStore := cachefile.GetSmartStore()
	if smartStore == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("smart cache not available"))
		return
	}

	if err := smartStore.FlushAll(); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.NoContent(w, r)
}

func flushSmartConfigCache(w http.ResponseWriter, r *http.Request) {
	configName := chi.URLParam(r, "config")
	if configName == "" {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("config name is required"))
		return
	}

	smartStore := cachefile.GetSmartStore()
	if smartStore == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("smart cache not available"))
		return
	}

	if err := smartStore.FlushByConfig(configName); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.NoContent(w, r)
}

func flushDnsCache(w http.ResponseWriter, r *http.Request) {
	resolver.ClearCache()
	render.NoContent(w, r)
}

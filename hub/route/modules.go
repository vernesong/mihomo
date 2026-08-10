package route

import (
	"errors"

	"github.com/metacubex/mihomo/component/modules"
	"github.com/metacubex/mihomo/hub/executor"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func moduleRouter() http.Handler {
	router := chi.NewRouter()
	router.Get("/", getModules)
	router.Route("/{name}", func(router chi.Router) {
		router.Get("/", getModule)
		if !embedMode {
			router.Patch("/", patchModule)
		}
	})
	return router
}

func getModules(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, render.M{
		"modules": executor.GetModules(),
	})
}

func getModuleConfig(w http.ResponseWriter, r *http.Request) {
	config, found := executor.GetModuleConfig()
	if !found {
		render.Status(r, http.StatusServiceUnavailable)
		render.JSON(w, r, newError("No active configuration"))
		return
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(config)
}

func getModule(w http.ResponseWriter, r *http.Request) {
	name := getEscapeParam(r, "name")
	module, found := executor.GetModule(name)
	if !found {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}
	render.JSON(w, r, module)
}

func patchModule(w http.ResponseWriter, r *http.Request) {
	name := getEscapeParam(r, "name")
	if _, found := executor.GetModule(name); !found {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}

	request := struct {
		Enable *bool `json:"enable"`
	}{}
	if err := render.DecodeJSON(r.Body, &request); err != nil || request.Enable == nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	if err := executor.SetModuleEnabled(name, *request.Enable); err != nil {
		if errors.Is(err, modules.ErrNotFound) {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, ErrNotFound)
			return
		}
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	render.NoContent(w, r)
}

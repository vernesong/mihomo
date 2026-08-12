package route

import (
	"errors"

	"github.com/metacubex/mihomo/component/script"
	"github.com/metacubex/mihomo/hub/executor"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func scriptRouter() http.Handler {
	router := chi.NewRouter()
	router.Get("/", getScripts)
	router.Route("/{name}", func(router chi.Router) {
		router.Get("/", getScript)
		if !embedMode {
			router.Patch("/", patchScript)
		}
	})
	return router
}

func getScripts(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, render.M{
		"scripts": executor.GetScripts(),
	})
}

func getScript(w http.ResponseWriter, r *http.Request) {
	name := getEscapeParam(r, "name")
	scriptInfo, found := executor.GetScript(name)
	if !found {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}
	render.JSON(w, r, scriptInfo)
}

func patchScript(w http.ResponseWriter, r *http.Request) {
	name := getEscapeParam(r, "name")
	if _, found := executor.GetScript(name); !found {
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

	if err := executor.SetScriptEnabled(name, *request.Enable); err != nil {
		if errors.Is(err, script.ErrNotFound) {
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

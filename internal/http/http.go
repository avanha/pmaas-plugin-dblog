package http

import (
	"embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"

	"pmaas.io/plugins/dblog/entities"
	"pmaas.io/plugins/dblog/internal/common"
	"pmaas.io/spi"
)

//go:embed content/static content/templates
var contentFS embed.FS

var loggedTrackableTemplate = spi.TemplateInfo{
	Name:   "logged_trackable",
	Paths:  []string{"templates/logged_trackable.htmlt"},
	Styles: []string{"css/logged_trackable.css"},
}

var statusTemplate = spi.TemplateInfo{
	Name:   "dblog_status",
	Paths:  []string{"templates/dblog_status.htmlt"},
	Styles: []string{"css/dblog_status.css"},
}

type Handler struct {
	container   spi.IPMAASContainer
	entityStore common.EntityStore
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) Init(container spi.IPMAASContainer, entityStore common.EntityStore) {
	h.container = container
	h.entityStore = entityStore
	container.ProvideContentFS(&contentFS, "content")
	container.EnableStaticContent("static")
	container.AddRoute("/plugins/dblog/", h.handleHttpListRequest)
	container.RegisterEntityRenderer(
		reflect.TypeOf((*entities.StatusEntity)(nil)).Elem(),
		h.statusEntityRendererFactory)
	container.RegisterEntityRenderer(
		reflect.TypeOf((*entities.LoggedTrackableEntity)(nil)).Elem(),
		h.loggedTrackableEntityRendererFactory)
}

func (h *Handler) handleHttpListRequest(writer http.ResponseWriter, request *http.Request) {
	result, err := h.entityStore.GetStatusAndEntities()

	if err != nil {
		fmt.Printf("dblog.http handleHttpListRequest: Error retrieving entities: %s\n", err)
		result = common.StatusAndEntities{}
	}

	// Convert the slice of structs to a slice of any
	entityListSize := len(result.Entities)
	entityPointers := make([]any, entityListSize)

	for i := 0; i < entityListSize; i++ {
		entityPointers[i] = &result.Entities[i]
	}

	sort.Slice(entityPointers, func(i, j int) bool {
		return entityPointers[i].(*entities.LoggedTrackableEntity).Name < entityPointers[j].(*entities.LoggedTrackableEntity).Name
	})

	h.container.RenderList(
		writer,
		request,
		spi.RenderListOptions{
			Title:  "dblog",
			Header: &result.Status,
		},
		entityPointers)
}

func (h *Handler) statusEntityRendererFactory() (spi.EntityRenderer, error) {
	// Load the template
	template, err := h.container.GetTemplate(&statusTemplate)

	if err != nil {
		return spi.EntityRenderer{}, fmt.Errorf("unable to load dblog_status template: %v", err)
	}

	// Declare a function that casts the entity to the expected type and evaluates it via the template loaded above
	renderer := func(w io.Writer, entity any) error {
		status, ok := entity.(*entities.StatusEntity)

		if !ok {
			return errors.New("item is not an instance of *StatusEntity")
		}

		err := template.Instance.Execute(w, status)

		if err != nil {
			return fmt.Errorf("unable to execute dblog_status template: %w", err)
		}

		return nil
	}

	return spi.EntityRenderer{
		StreamingRenderFunc: renderer,
		Styles:              template.Styles,
		Scripts:             template.Scripts,
	}, nil
}

func (h *Handler) loggedTrackableEntityRendererFactory() (spi.EntityRenderer, error) {
	// Load the template
	template, err := h.container.GetTemplate(&loggedTrackableTemplate)

	if err != nil {
		return spi.EntityRenderer{}, fmt.Errorf("unable to load logged_trackable template: %v", err)
	}

	// Declare a function that casts the entity to the expected type and evaluates it via the template loaded above
	renderer := func(w io.Writer, entity any) error {
		loggedTrackable, ok := entity.(*entities.LoggedTrackableEntity)

		if !ok {
			return errors.New("item is not an instance of *LoggedTrackableEntity")
		}

		err := template.Instance.Execute(w, loggedTrackable)

		if err != nil {
			return fmt.Errorf("unable to execute logged_trackable template: %w", err)
		}

		return nil
	}

	return spi.EntityRenderer{
		StreamingRenderFunc: renderer,
		Styles:              template.Styles,
		Scripts:             template.Scripts,
	}, nil
}

package dblog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	config2 "pmaas.io/plugins/dblog/config"
	"pmaas.io/plugins/dblog/entities"
	"pmaas.io/plugins/dblog/internal/common"
	"pmaas.io/plugins/dblog/internal/http"
	"pmaas.io/plugins/dblog/internal/poller"
	"pmaas.io/plugins/dblog/internal/writer"
	"pmaas.io/spi"
	"pmaas.io/spi/events"
	"pmaas.io/spi/tracking"
)

type stats struct {
	successCount       int
	failureCount       int
	lastFailureTime    time.Time
	lastFailureMessage string
	retryQueueSize     int
}

type plugin struct {
	config               config2.PluginConfig
	container            spi.IPMAASContainer
	eventReceiverHandles map[string]int
	entities             map[string]*trackableWrapper
	pollers              sync.WaitGroup
	ctx                  context.Context
	cancel               context.CancelFunc
	writers              sync.WaitGroup
	writeRequestCh       chan writer.Request
	stopEvents           chan func()
	httpHandler          *http.Handler
	statusStarted        bool
	statusStartupSuccess bool
	statusDbDown         bool
	stats                stats
}

type Plugin interface {
	spi.IPMAASPlugin2
}

func NewPlugin(config config2.PluginConfig) Plugin {
	fmt.Printf("New, config: %v\n", config)
	instance := &plugin{
		config:               config,
		eventReceiverHandles: make(map[string]int),
		entities:             make(map[string]*trackableWrapper),
		httpHandler:          http.NewHandler(),
	}

	return instance
}

func (p *plugin) Init(container spi.IPMAASContainer) {
	p.container = container
	p.httpHandler.Init(container, &entityStoreAdapter{parent: p})
}

func (p *plugin) Start() {
	p.pollers = sync.WaitGroup{}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.startWriters()
	p.registerEventHandlers()
	// TODO: Retrieve the list of possible entities to add to our map.
	// Without it, we depend on the plugin ordering to ensure we get any devices in existence prior to our registration.
	p.statusStarted = true
	p.statusStartupSuccess = p.stats.lastFailureTime.IsZero()
}

func (p *plugin) Stop() {

}

func (p *plugin) StopAsync() chan func() {
	fmt.Printf("%T Stopping...\n", p)
	p.cancel()

	// We don't want to block on the pluginRunner goroutine, so start a new goroutine to wait
	// on the WaitGroup and issue a callback once that's done.
	p.stopEvents = make(chan func())
	go func() {
		fmt.Printf("%T Waiting for pollers...\n", p)
		p.pollers.Wait()
		p.stopEvents <- p.onPollerGoRoutinesStopped
	}()
	return p.stopEvents
}

// getStatusAndEntities retrieves the plugin status and a list of currently registered trackable entities.
// It does not perform any synchronization, so it should only be called from the plugin's main GoRoutine.
func (p *plugin) getStatusAndEntities() common.StatusAndEntities {
	trackables := make([]entities.LoggedTrackableEntity, len(p.entities))
	i := 0
	for _, entity := range p.entities {
		trackables[i] = entity.toLoggedTrackableEntity()
		i++
	}
	return common.StatusAndEntities{
		Status: entities.StatusEntity{
			DriverName:         p.config.DriverName,
			DbName:             sanitizeDataSource(p.config.DataSourceName),
			Status:             p.calculateStatus(),
			SuccessCount:       p.stats.successCount,
			FailureCount:       p.stats.failureCount,
			LastFailureTime:    p.stats.lastFailureTime,
			LastFailureMessage: p.stats.lastFailureMessage,
			RetryQueueSize:     p.stats.retryQueueSize,
		},
		Entities: trackables,
	}
}

func (p *plugin) calculateStatus() string {
	if !p.statusStarted {
		return "Starting..."
	}

	if !p.statusStartupSuccess {
		return "Startup failed"
	}

	if p.statusDbDown {
		return "Running, DB error"
	}

	return "Running"
}

var passwordRE = regexp.MustCompile("password=\\w+")

func sanitizeDataSource(dataSource string) string {
	return passwordRE.ReplaceAllString(dataSource, "password=****")
}

func (p *plugin) onPollerGoRoutinesStopped() {
	fmt.Printf("%T Poller goroutines stopped, deregistering entities...\n", p)
	p.deregisterEventHandlers()
	// TODO: Clear our map of entities

	if p.writeRequestCh == nil {
		p.onStopComplete()
		return
	}

	fmt.Printf("%T Stopping writer(s)...\n", p)

	close(p.writeRequestCh)
	p.writeRequestCh = nil

	go func() {
		fmt.Printf("%T Waiting for writers...\n", p)
		p.writers.Wait()
		p.stopEvents <- p.onWriterGoRoutinesStopped
	}()
}

func (p *plugin) onWriterGoRoutinesStopped() {
	fmt.Printf("%T Writer goroutines stopped\n", p)
	p.onStopComplete()
}

func (p *plugin) onStopComplete() {
	fmt.Printf("%T Stopped\n", p)
	close(p.stopEvents)
	p.stopEvents = nil
}

func (p *plugin) startWriters() {
	p.writers = sync.WaitGroup{}
	// Allow pollers to submit requests to the writer without blocking.
	// Polling should generally occur less frequently, and the writer should
	// be able to keep up.
	writeRequestCh := make(chan writer.Request, 10)
	connectionFactoryFn := func() (db *sql.DB, err error) {
		fmt.Printf("dblog.DbWriter connecting to %s database: %s\n", p.config.DriverName, p.config.DataSourceName)
		return sql.Open(p.config.DriverName, p.config.DataSourceName)
	}
	writerStatsTracker := &writerStatsTrackerAdapter{parent: p}
	task := writer.CreateDbWriter(p.ctx, p.config, connectionFactoryFn, writeRequestCh, writerStatsTracker)
	initErrorCh := make(chan error)
	p.writers.Go(func() {
		err := task.Init()

		if err != nil {
			initErrorCh <- err
		}

		close(initErrorCh)

		if err == nil {
			task.Run()
		}
	})

	// Wait for initialization to complete
	err := <-initErrorCh

	if err == nil {
		fmt.Printf("%T DbWriter started\n", p)
		p.writeRequestCh = writeRequestCh
	} else {
		message := fmt.Sprintf("Failed to start DbWriter: %v", err)
		p.statusDbDown = true
		p.stats.lastFailureTime = time.Now()
		p.stats.lastFailureMessage = message
		fmt.Printf("%T %s\n", p, message)
	}
}

func (p *plugin) registerEventHandlers() {
	var handle int
	var err error

	handle, err = p.container.RegisterEventReceiver(
		p.onEntityRegisteredPredicate,
		p.onEntityRegistered,
	)

	if err != nil {
		panic(fmt.Errorf("unable to register for entity registration events: %v", err))
	}

	p.eventReceiverHandles["onEntityDeregistered"] = handle

	handle, err = p.container.RegisterEventReceiver(
		p.onEntityDeregisteredPredicate,
		p.onEntityDeregistered,
	)

	if err != nil {
		panic(fmt.Errorf("unable to register for entity deregistration events: %v", err))
	}

	p.eventReceiverHandles["onEntityDeregistered"] = handle

}

func (p *plugin) deregisterEventHandlers() {
	deregisteredEvents := make([]string, 0, len(p.eventReceiverHandles))
	for eventName, handle := range p.eventReceiverHandles {
		err := p.container.DeregisterEventReceiver(handle)

		if err == nil {
			deregisteredEvents = append(deregisteredEvents, eventName)
		} else {
			fmt.Printf("Unable to deregister handler for event %s: %v", eventName, err)
		}
	}

	for _, eventName := range deregisteredEvents {
		delete(p.eventReceiverHandles, eventName)
	}

	registrationCount := len(p.eventReceiverHandles)

	if registrationCount > 0 {
		fmt.Printf("Unable to deregister all event handlers, %d remaining: %v",
			registrationCount, p.eventReceiverHandles)
	}
}

func (p *plugin) onEntityRegisteredPredicate(eventInfo *events.EventInfo) bool {
	// We only want entity registrations
	entityRegisteredEvent, ok := eventInfo.Event.(events.EntityRegisteredEvent)

	if !ok {
		return false
	}

	// We only want Trackable entities
	compatible := entityRegisteredEvent.EntityType.AssignableTo(tracking.TrackableType)

	return compatible
}

func (p *plugin) onEntityRegistered(eventInfo *events.EventInfo) error {
	fmt.Printf("%T onEntityRegistered(%v)\n", p, eventInfo)
	event := eventInfo.Event.(events.EntityRegisteredEvent)
	_, ok := p.entities[event.Id]

	if ok {
		fmt.Printf("Entity %s already tracked\n", event.Id)
		return nil
	}

	if event.StubFactoryFn == nil {
		fmt.Printf("Entity %s has no stub factory\n", event.Id)
		return nil
	}

	entityStub, err := event.StubFactoryFn()

	if err != nil {
		return fmt.Errorf("unable to create stub for entity %s: %w", event.Id, err)
	}

	entity := entityStub.(tracking.Trackable)
	trackingConfig := entity.TrackingConfig()

	if trackingConfig.TrackingMode == 0 {
		fmt.Printf("Entity %s has not enabled tracking\n", event.Id)
		return nil
	}

	wrapped := &trackableWrapper{
		trackable:        entity,
		registrationTime: time.Now(),
		trackingConfig:   trackingConfig,
		id:               event.Id,
		name:             trackingConfig.Name,
	}
	p.entities[event.Id] = wrapped

	switch trackingConfig.TrackingMode {
	case tracking.ModePoll:
		statsTrackerAdapter := &pollerStatsTrackerAdapter{
			parent:        p,
			entityWrapper: wrapped,
		}
		taskCtx, pollTaskCancelFn := context.WithCancel(p.ctx)
		wrapped.pollTaskCancelFn = pollTaskCancelFn
		task := poller.NewTask(taskCtx, entity, trackingConfig, p.pollerWriteRequestHandlerFn, statsTrackerAdapter)
		p.pollers.Go(task.Run)
		break
	case tracking.ModePush:
		// TODO: Register a broadcast receiver for the entity
		break
	default:
		fmt.Printf("Unsupported tracking mode: %v\n", trackingConfig.TrackingMode)
	}

	return nil
}

func (p *plugin) onEntityDeregisteredPredicate(eventInfo *events.EventInfo) bool {
	// We only want entity deregistrations
	entityDeregisteredEvent, ok := eventInfo.Event.(events.EntityDeregisteredEvent)

	if !ok {
		return false
	}

	// We only want Trackable entities
	compatible := entityDeregisteredEvent.EntityType.AssignableTo(tracking.TrackableType)

	return compatible
}

func (p *plugin) onEntityDeregistered(eventInfo *events.EventInfo) error {
	fmt.Printf("%T onEntityDeregistered(%v)\n", p, eventInfo)
	event := eventInfo.Event.(events.EntityDeregisteredEvent)
	wrapped, ok := p.entities[event.Id]

	if !ok {
		fmt.Printf("Entity %s not tracked\n", event.Id)
		return nil
	}

	wrapped.pollTaskCancelFn()
	delete(p.entities, event.Id)

	return nil
}

func (p *plugin) pollerWriteRequestHandlerFn(request writer.Request) error {
	errorCh := make(chan error)

	err := p.container.EnqueueOnPluginGoRoutine(func() {
		errorCh <- p.submitWriteRequest(request)
		close(errorCh)
	})

	if err != nil {
		close(errorCh)
		return err
	}

	return <-errorCh
}

func (p *plugin) submitWriteRequest(request writer.Request) error {
	if p.writeRequestCh == nil {
		return errors.New("write request channel not initialized")
	}

	p.writeRequestCh <- request

	return nil
}

func (p *plugin) onDbWriterSuccess(retryQueueSize int) {
	p.stats.successCount++
	p.stats.retryQueueSize = retryQueueSize

	if p.statusDbDown {
		p.statusDbDown = false
	}
}

func (p *plugin) onDbWriterFailure(err error, retryQueueSize int) {
	p.stats.failureCount++
	p.stats.lastFailureTime = time.Now()
	p.stats.lastFailureMessage = err.Error()
	p.stats.retryQueueSize = retryQueueSize

	if !p.statusDbDown {
		p.statusDbDown = true
	}
}

func (p *plugin) onDbWriterRetryQueueSizeChange(retryQueueSize int) {
	p.stats.retryQueueSize = retryQueueSize
}

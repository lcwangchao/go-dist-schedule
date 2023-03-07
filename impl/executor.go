// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package impl

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lcwangchao/godistschedule/api"
	"github.com/lcwangchao/godistschedule/meta"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var errTaskNotAssigned = errors.New("task is not assigned to current agent")

const (
	statusCreated int32 = iota
	statusRunning
	statusClosed
)

type taskStateChanges struct {
	NeedDoCheckpoint    bool
	Checkpoint          []byte
	CheckpointDone      chan<- *api.CheckpointResult
	NeedReleaseTask     bool
	NeedTerminateTask   bool
	TaskTerminateReason string
}

type taskDriver struct {
	sync.RWMutex
	taskID            string
	task              *meta.TaskMeta
	released          bool
	terminated        bool
	terminateReason   string
	checkpoint        []byte
	checkpointDone    chan *api.CheckpointResult
	stateChangeNotify chan<- struct{}
}

func newTaskDriver(task *meta.TaskMeta, stateChangeNotify chan<- struct{}) *taskDriver {
	return &taskDriver{
		taskID:            task.ID,
		task:              task,
		stateChangeNotify: stateChangeNotify,
	}
}

func (d *taskDriver) TaskID() string {
	return d.taskID
}

func (d *taskDriver) TaskMeta() *meta.TaskMeta {
	d.RLock()
	defer d.RUnlock()
	return d.task
}

func (d *taskDriver) Checkpoint(cp []byte) <-chan *api.CheckpointResult {
	d.Lock()
	if d.released || d.terminated {
		d.Unlock()
		ch := make(chan *api.CheckpointResult, 1)
		ch <- &api.CheckpointResult{Err: errors.New("task has been released or terminated")}
		close(ch)
		return ch
	}

	d.checkpoint = cp
	if d.checkpointDone == nil {
		d.checkpointDone = make(chan *api.CheckpointResult, 1)
	}
	done := d.checkpointDone
	d.Unlock()

	d.notifyStateChange()
	return done
}

func (d *taskDriver) ReleaseTask() {
	d.Lock()
	if d.released || d.terminated {
		d.Unlock()
		return
	}
	d.released = true
	d.Unlock()
	d.notifyStateChange()
}

func (d *taskDriver) TerminateTask(reason string) {
	d.Lock()
	if d.released || d.terminated {
		d.Unlock()
		return
	}

	if reason == "" {
		reason = meta.TaskTerminateUnknown
	}

	d.terminated = true
	d.terminateReason = reason
	d.Unlock()
	d.notifyStateChange()
}

func (d *taskDriver) IsValid() bool {
	d.RLock()
	defer d.RUnlock()
	return d.released || d.terminated
}

func (d *taskDriver) notifyStateChange() {
	select {
	case d.stateChangeNotify <- struct{}{}:
	default:
	}
}

func (d *taskDriver) PollTaskChanges() *taskStateChanges {
	d.Lock()
	defer d.Unlock()
	if !d.released && !d.terminated && d.checkpointDone == nil {
		return nil
	}

	changes := &taskStateChanges{
		NeedDoCheckpoint:    d.checkpointDone != nil,
		Checkpoint:          d.checkpoint,
		CheckpointDone:      d.checkpointDone,
		NeedReleaseTask:     d.released,
		NeedTerminateTask:   d.terminated,
		TaskTerminateReason: d.terminateReason,
	}

	if changes.NeedDoCheckpoint {
		d.checkpointDone = nil
		d.checkpoint = nil
	}

	return changes
}

type executorDriver struct {
	ctx              context.Context
	close            func()
	closed           chan struct{}
	agent            meta.AgentEpoch
	store            meta.Storage
	executorID       string
	executor         api.Executor
	candidatesNotify chan any
	driverNotify     chan struct{}

	candidates         atomic.Pointer[[]*meta.TaskMeta]
	tasks              sync.Map
	pendingRemoveTasks sync.Map
	status             atomic.Int32
	logger             *zap.Logger
}

func newExecutorDriver(store meta.Storage, agent meta.AgentEpoch, executorID string, executor api.Executor) *executorDriver {
	ctx, cancel := context.WithCancel(context.Background())
	return &executorDriver{
		ctx:              ctx,
		close:            cancel,
		closed:           make(chan struct{}),
		agent:            agent,
		store:            store,
		executorID:       executorID,
		executor:         executor,
		candidatesNotify: make(chan any, 1),
		driverNotify:     make(chan struct{}, 1),
		logger:           log.With(zap.String("executorID", executorID), zap.String("agent", agent.ID), zap.String("epochID", agent.EpochID)),
	}
}

func (e *executorDriver) Start() {
	e.status.CompareAndSwap(statusCreated, statusRunning)
	go e.loop()
}

func (e *executorDriver) Close() {
	if e.status.CompareAndSwap(statusCreated, statusClosed) {
		e.close()
		close(e.closed)
		return
	}

	if e.status.CompareAndSwap(statusRunning, statusClosed) {
		e.close()
	}
}

func (e *executorDriver) Closed() bool {
	select {
	case <-e.closed:
		return true
	default:
		return false
	}
}

func (e *executorDriver) WaitClosed(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		return nil
	}
}

func (e *executorDriver) OfferCandidateTasks(tasks []*meta.TaskMeta) {
	if e.ctx.Err() != nil || len(tasks) == 0 {
		return
	}
	e.candidates.Swap(&tasks)
	select {
	case e.candidatesNotify <- struct{}{}:
	default:
	}
}

func (e *executorDriver) loop() {
	e.executor.Init(&api.ExecutorContext{
		AgentEpoch: e.agent,
		Client:     NewDefaultClient(e.store),
	})

	e.logger.Info("executor loaded")
	scheduleCandidatesTicker := time.Tick(5 * time.Second)
	processOwnTasksTicker := time.Tick(5 * time.Second)
	handlePendingRemoveTasksTicker := time.Tick(10 * time.Second)
loop:
	for {
		select {
		case <-e.ctx.Done():
			break loop
		case <-scheduleCandidatesTicker:
			e.scheduleCandidateTasks(e.ctx)
		case <-processOwnTasksTicker:
			e.processOwnTasks(e.ctx)
		case <-handlePendingRemoveTasksTicker:
			e.handlePendingRemoveTasks(e.ctx)
		case <-e.candidatesNotify:
			e.scheduleCandidateTasks(e.ctx)
		case <-e.driverNotify:
			e.processOwnTasks(e.ctx)
		}
	}
	e.closeAndWaitTasksReleased()
}

func (e *executorDriver) scheduleCandidateTasks(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	var candidates []*meta.TaskMeta
	if p := e.candidates.Swap(nil); p != nil {
		candidates = *p
	}

	if len(candidates) == 0 {
		return
	}

	for _, task := range candidates {
		if _, ok := e.tasks.Load(task.ID); ok {
			continue
		}

		if _, ok := e.pendingRemoveTasks.Load(task.ID); ok {
			continue
		}

		if !e.executor.ShouldLaunchTask(task) {
			continue
		}

		driver, ok := e.obtainTask(ctx, task)
		if !ok {
			continue
		}

		e.executor.LaunchTask(driver.task, driver)
		e.tasks.Store(task.ID, driver)
	}
}

func (e *executorDriver) processOwnTasks(ctx context.Context) {
	e.tasks.Range(func(_, value any) bool {
		driver := value.(*taskDriver)
		taskID := driver.taskID
		changes := driver.PollTaskChanges()
		if changes == nil {
			return true
		}

		if changes.NeedDoCheckpoint {
			if err := e.doCheckpoint(ctx, taskID, changes.Checkpoint, changes.CheckpointDone); err != nil {
				if err == errTaskNotAssigned {
					e.executor.EvictTask(taskID, api.EvictReasonUnknown)
					return true
				}
			}
		}

		var err error
		switch {
		case changes.NeedTerminateTask:
			e.tasks.Delete(taskID)
			if err = e.terminateTask(ctx, taskID, changes.TaskTerminateReason); err != nil {
				e.pendingRemoveTasks.Store(taskID, changes)
			}
		case changes.NeedReleaseTask:
			e.tasks.Delete(taskID)
			if err = e.releaseTask(ctx, taskID); err != nil {
				e.pendingRemoveTasks.Store(taskID, changes)
			}
		}

		return true
	})
}

func (e *executorDriver) obtainTask(ctx context.Context, task *meta.TaskMeta) (*taskDriver, bool) {
	filter := e.taskIDFilter(task.ID, &task.Agent)
	update := &meta.TaskMetaUpdate{
		SetAgent:  &e.agent,
		SetStatus: meta.TaskStatusRunning,
	}

	id, err := e.store.UpdateTask(ctx, filter, update)
	if err != nil {
		e.logger.Error("lock task occurs error", zap.Error(err), zap.String("taskID", task.ID))
	} else if len(id) == 0 {
		return nil, false
	}

	newTask, err := e.getOwnTask(ctx, task.ID)
	if err != nil {
		e.logger.Error("get task occurs error", zap.Error(err), zap.String("taskID", task.ID))
		e.pendingRemoveTasks.Store(task.ID, &taskStateChanges{NeedReleaseTask: true})
		return nil, false
	}

	if newTask.Agent != e.agent {
		return nil, false
	}

	return newTaskDriver(newTask, e.driverNotify), true
}

func (e *executorDriver) releaseTask(ctx context.Context, taskID string) error {
	filter := e.taskIDFilter(taskID, &e.agent)
	update := &meta.TaskMetaUpdate{
		SetAgent:  &meta.AgentEpoch{},
		SetStatus: meta.TaskStatusWaiting,
	}

	if _, err := e.store.UpdateTask(ctx, filter, update); err != nil {
		e.logger.Error("release task occurs error", zap.Error(err), zap.String("taskID", taskID))
		return err
	}

	return nil
}

func (e *executorDriver) getOwnTask(ctx context.Context, taskID string) (*meta.TaskMeta, error) {
	filter := e.taskIDFilter(taskID, &e.agent)
	tasks, err := e.store.ListTasks(ctx, filter)
	if err != nil {
		return nil, err
	}

	if len(tasks) == 0 {
		return nil, meta.ErrNotFound
	}

	return tasks[0], nil
}

func (e *executorDriver) taskIDFilter(id string, agent *meta.AgentEpoch) *meta.TaskFilter {
	return &meta.TaskFilter{
		ID: map[string]struct{}{
			id: {},
		},
		Agent: agent,
	}
}

func (e *executorDriver) updateSelfTask(ctx context.Context, taskID string, update *meta.TaskMetaUpdate) error {
	filter := e.taskIDFilter(taskID, &e.agent)
	result, err := e.store.UpdateTask(ctx, filter, update)
	if err != nil {
		return err
	}

	if len(result) == 0 {
		return errTaskNotAssigned
	}

	return nil
}

func (e *executorDriver) doCheckpoint(ctx context.Context, taskID string, checkpoint []byte, done chan<- *api.CheckpointResult) error {
	defer close(done)

	err := e.updateSelfTask(ctx, taskID, &meta.TaskMetaUpdate{
		SetCheckpoint: true,
		CheckPoint:    checkpoint,
	})

	if err != nil {
		e.logger.Error("task do checkpoint occurs error", zap.Error(err), zap.String("taskID", taskID))
		done <- &api.CheckpointResult{Err: err}
		return err
	}

	val, _ := e.tasks.Load(taskID)
	driver := val.(*taskDriver)
	task := driver.TaskMeta()
	task = task.Clone()
	task.CheckPoint = make([]byte, len(checkpoint))
	copy(task.CheckPoint, checkpoint)
	driver.Lock()
	driver.task = task
	driver.Unlock()
	done <- &api.CheckpointResult{}
	return nil
}

func (e *executorDriver) terminateTask(ctx context.Context, taskID string, reason string) error {
	err := e.updateSelfTask(ctx, taskID, &meta.TaskMetaUpdate{
		SetStatus:          meta.TaskStatusTerminated,
		SetTerminateReason: reason,
	})

	if err != nil {
		e.logger.Error("task terminate occurs error", zap.Error(err), zap.String("taskID", taskID))
		return err
	}

	return nil
}

func (e *executorDriver) handlePendingRemoveTasks(ctx context.Context) {
	e.pendingRemoveTasks.Range(func(key, val any) bool {
		taskID := key.(string)
		changes := val.(*taskStateChanges)

		var err error
		switch {
		case changes.NeedReleaseTask:
			err = e.releaseTask(ctx, taskID)
		case changes.NeedTerminateTask:
			err = e.terminateTask(ctx, taskID, changes.TaskTerminateReason)
		}

		if err == nil {
			e.pendingRemoveTasks.Delete(key)
		}

		return true
	})
}

func (e *executorDriver) closeAndWaitTasksReleased() {
	e.logger.Info("close executor and wait all tasks released")
	defer close(e.closed)

	e.executor.Close()
	for {
		allReleased := true
		e.tasks.Range(func(key, value any) bool {
			taskID := key.(string)
			driver := value.(*taskDriver)
			if !driver.IsValid() {
				e.pendingRemoveTasks.Store(taskID, driver.PollTaskChanges())
			} else {
				allReleased = false
			}
			return true
		})

		e.handlePendingRemoveTasks(context.Background())
		if allReleased {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

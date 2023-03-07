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
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lcwangchao/godistschedule/api"
	"github.com/lcwangchao/godistschedule/meta"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const hbInterval = 10 * time.Second
const taskDetectInterval = 5 * time.Second

type nodeAgent struct {
	sync.RWMutex
	id        string
	meta      *meta.AgentMeta
	online    bool
	store     meta.Storage
	factories map[string]func() api.Executor
	executors map[string]*executorDriver
	status    meta.AgentStatus
	ctx       context.Context
	stop      func()
}

func NewNodeAgent(id string, store meta.Storage) api.Agent {
	return &nodeAgent{
		id:        id,
		store:     store,
		factories: make(map[string]func() api.Executor),
		status:    meta.AgentStatusNew,
	}
}

func (n *nodeAgent) RegisterExecutor(executorID string, factory func() api.Executor) error {
	n.Lock()
	defer n.Unlock()
	if _, ok := n.factories[executorID]; ok {
		return meta.ErrDuplicatedID
	}

	n.factories[executorID] = factory
	if n.status == meta.AgentStatusRunning && n.online {
		n.startExecutor(executorID, factory)
	}

	return nil
}

func (n *nodeAgent) Meta() *meta.AgentMeta {
	n.RLock()
	defer n.RUnlock()
	return n.meta
}

func (n *nodeAgent) Online() bool {
	n.RLock()
	defer n.RUnlock()
	return n.online
}

func (n *nodeAgent) Status() meta.AgentStatus {
	n.RLock()
	defer n.RUnlock()
	return n.status
}

func (n *nodeAgent) Start() {
	n.Lock()
	defer n.Unlock()
	if n.status != meta.AgentStatusNew {
		return
	}

	n.ctx, n.stop = context.WithCancel(context.Background())
	go n.hbLoop()
	n.status = meta.AgentStatusStarting
}

func (n *nodeAgent) Stop() {
	n.Lock()
	defer n.Unlock()
	switch n.status {
	case meta.AgentStatusStopping, meta.AgentStatusStopped:
		return
	}

	if n.stop != nil {
		n.stop()
		n.status = meta.AgentStatusStopping
	} else {
		n.status = meta.AgentStatusStopped
	}
}

func (n *nodeAgent) hbLoop() {
	var wg sync.WaitGroup
	wg.Add(1)
	go n.taskDetectionLoop(&wg)
	n.doHeartbeatOnce(n.ctx)
	ticker := time.Tick(hbInterval)
loop:
	for {
		select {
		case <-n.ctx.Done():
			break loop
		case <-ticker:
			n.doHeartbeatOnce(n.ctx)
		}
	}

	n.RLock()
	waitCloseFuncs := make([]func(ctx context.Context) error, 0, len(n.executors))
	for _, driver := range n.executors {
		driver.Close()
		waitCloseFuncs = append(waitCloseFuncs, driver.WaitClosed)
	}
	n.RUnlock()

	for _, fn := range waitCloseFuncs {
		_ = fn(context.Background())
	}

	wg.Wait()
	n.Lock()
	n.status = meta.AgentStatusStopped
	n.Unlock()
}

func (n *nodeAgent) taskDetectionLoop(wg *sync.WaitGroup) {
	defer wg.Add(-1)

	ticker := time.Tick(taskDetectInterval)
	watcher := n.store.WatchTask(n.ctx, "", true)
loop:
	for {
		select {
		case <-n.ctx.Done():
			break loop
		case resp := <-watcher:
			for _, e := range resp.Events {
				if e.Tp == meta.TaskCreated {
					n.detectAndOfferTasks(n.ctx)
					break
				}
			}
		case <-ticker:
			n.detectAndOfferTasks(n.ctx)
		}
	}
}

func (n *nodeAgent) detectAndOfferTasks(ctx context.Context) {
	if !n.Online() {
		return
	}

	expiredTime := time.Now().Add(-5 * hbInterval)
	tasks, err := n.store.ListTasks(ctx, &meta.TaskFilter{
		Status:                       meta.TaskStatusWaiting,
		ExpiredAgentTimeAsWaitStatus: &expiredTime,
	})

	if err != nil {
		log.Error("cannot detect waiting tasks", zap.String("agent", n.id), zap.Error(err))
		return
	}

	if len(tasks) == 0 {
		return
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ExecutorID < tasks[j].ExecutorID
	})

	for start, i := 0, 1; i <= len(tasks); i++ {
		if i == len(tasks) || tasks[i].ExecutorID != tasks[start].ExecutorID {
			n.RLock()
			executor, ok := n.executors[tasks[start].ExecutorID]
			n.RUnlock()
			if ok {
				executor.OfferCandidateTasks(tasks[start:i])
			}
			start = i
		}
	}
}

func (n *nodeAgent) doHeartbeatOnce(ctx context.Context) {
	if !n.Online() {
		n.RLock()
		// waiting for all tasks are evicted
		for _, driver := range n.executors {
			if !driver.Closed() {
				n.RUnlock()
				return
			}
		}

		n.RUnlock()
		n.newEpoch(n.ctx)
		return
	}

	renewCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n.heartbeatCurrentEpoch(renewCtx)
}

func (n *nodeAgent) newEpoch(ctx context.Context) {
	agents, err := n.store.ListAgents(ctx, n.id)
	if err != nil {
		log.Error(
			"cannot get agent meta when stepping to a new epoch",
			zap.String("agent", n.id),
			zap.Error(err),
		)
		return
	}

	var agent *meta.AgentMeta
	if len(agents) > 0 {
		agent = agents[0].Clone()
	}

	if agent == nil {
		agent = &meta.AgentMeta{
			AgentEpoch: meta.AgentEpoch{
				ID:      n.id,
				EpochID: uuid.NewString(),
			},
			HeartbeatTime: time.Now(),
			Status:        meta.AgentStatusRunning,
		}

		if err = n.store.CreateAgent(ctx, agent); err != nil {
			log.Error(
				"failed to create agent meta",
				zap.String("agent", agent.ID),
				zap.String("epoch", agent.EpochID),
				zap.Error(err),
			)
			return
		}
	} else {
		agent.HeartbeatTime = time.Now()
		agent.Status = meta.AgentStatusRunning
		agent.EpochID = uuid.NewString()

		err = n.store.UpdateAgent(ctx, n.id, &meta.AgentMetaUpdate{
			SetEpochID:       agent.EpochID,
			SetHeartbeatTime: &agent.HeartbeatTime,
			SetStatus:        agent.Status,
		})

		if err != nil {
			log.Error(
				"failed to update agent meta with new epoch",
				zap.String("agent", agent.ID),
				zap.String("epoch", agent.EpochID),
				zap.Error(err),
			)
			return
		}
	}

	n.Lock()
	defer n.Unlock()

	if n.status == meta.AgentStatusStarting {
		n.status = meta.AgentStatusRunning
	}

	if n.status != meta.AgentStatusRunning {
		return
	}

	n.meta = agent
	n.online = true
	n.executors = make(map[string]*executorDriver, len(n.factories))
	for executorID, f := range n.factories {
		n.startExecutor(executorID, f)
	}
}

func (n *nodeAgent) heartbeatCurrentEpoch(ctx context.Context) {
	agent := n.Meta()
	defer func() {
		if time.Since(agent.HeartbeatTime) > 3*hbInterval {
			log.Error(
				"current agent epoch expired, set agent to offline",
				zap.String("agent", agent.ID),
				zap.String("epochID", agent.EpochID),
				zap.Time("lastHeartbeatTime", agent.HeartbeatTime),
			)
			n.Lock()
			n.online = false
			for _, d := range n.executors {
				d.Close()
			}
			n.Unlock()
		}
	}()

	setHeartbeatTime := time.Now()
	err := n.store.UpdateAgent(ctx, n.id, &meta.AgentMetaUpdate{
		SetHeartbeatTime: &setHeartbeatTime,
	})

	if err != nil {
		log.Error("heartbeat current epoch failed",
			zap.String("agent", n.id),
			zap.String("epochID", agent.EpochID),
			zap.Error(err),
		)
		return
	}

	agent = agent.Clone()
	agent.HeartbeatTime = setHeartbeatTime

	n.Lock()
	n.meta = agent
	n.Unlock()
}

func (n *nodeAgent) startExecutor(executorID string, f func() api.Executor) {
	driver := newExecutorDriver(n.store, n.meta.AgentEpoch, executorID, f())
	driver.Start()
	n.executors[executorID] = driver
}

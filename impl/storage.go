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
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lcwangchao/godistschedule/meta"
)

type mockMetaStorage struct {
	sync.RWMutex
	id2Task    map[string]*meta.TaskMeta
	id2Agent   map[string]*meta.AgentMeta
	namespaces map[string]struct {
		key2Task map[string]*meta.TaskMeta
	}
	watchChList []struct {
		ch     chan *meta.WatchTaskResponse
		key    string
		prefix bool
	}
}

func NewMockMetaStorage() meta.Storage {
	return &mockMetaStorage{
		id2Task:  make(map[string]*meta.TaskMeta),
		id2Agent: make(map[string]*meta.AgentMeta),
		namespaces: make(map[string]struct {
			key2Task map[string]*meta.TaskMeta
		}),
	}
}

func (s *mockMetaStorage) CreateTask(_ context.Context, task *meta.TaskMeta) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.id2Task[task.ID]; ok {
		return meta.ErrDuplicatedID
	}

	ns, ok := s.namespaces[task.Namespace]
	if ok {
		if _, ok = ns.key2Task[task.Key]; ok {
			return meta.ErrDuplicatedKey
		}
	} else {
		ns = struct{ key2Task map[string]*meta.TaskMeta }{key2Task: make(map[string]*meta.TaskMeta)}
		s.namespaces[task.Namespace] = ns
	}

	task = task.Clone()
	s.id2Task[task.ID] = task
	ns.key2Task[task.Key] = task
	s.emitTaskEvent(task.ID, task.Key, meta.TaskCreated)
	return nil
}

func (s *mockMetaStorage) ListTasks(_ context.Context, filter *meta.TaskFilter) ([]*meta.TaskMeta, error) {
	s.RLock()
	defer s.RUnlock()
	tasks := s.filterTasks(filter)
	for i, task := range tasks {
		tasks[i] = task.Clone()
	}
	return tasks, nil
}

func (s *mockMetaStorage) DeleteTask(_ context.Context, filter *meta.TaskFilter) ([]string, error) {
	s.Lock()
	defer s.Unlock()
	tasks := s.filterTasks(filter)
	idList := make([]string, 0, len(tasks))
	for _, task := range tasks {
		idList = append(idList, task.ID)
		delete(s.id2Task, task.ID)
		if ns, ok := s.namespaces[task.Namespace]; ok {
			delete(ns.key2Task, task.Key)
			if len(ns.key2Task) == 0 {
				delete(s.namespaces, task.Namespace)
			}
			s.emitTaskEvent(task.ID, task.Key, meta.TaskDeleted)
		}
	}
	return idList, nil
}

func (s *mockMetaStorage) UpdateTask(_ context.Context, filter *meta.TaskFilter, update *meta.TaskMetaUpdate) ([]string, error) {
	s.Lock()
	defer s.Unlock()
	tasks := s.filterTasks(filter)
	idList := make([]string, 0, len(tasks))
	for _, task := range tasks {
		idList = append(idList, task.ID)
		if update.SetAgent != nil {
			task.Agent = *update.SetAgent
		}

		if update.SetStatus != "" {
			task.Status = update.SetStatus
			task.TerminateReason = update.SetTerminateReason
		}

		if update.SetCheckpoint {
			cp := make([]byte, len(update.CheckPoint))
			copy(cp, update.CheckPoint)
			task.CheckPoint = cp
		}

		s.emitTaskEvent(task.ID, task.Key, meta.TaskUpdated)
	}
	return idList, nil
}

func (s *mockMetaStorage) emitTaskEvent(taskID, taskKey string, eventTp meta.TaskEventTp) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	r.Shuffle(len(s.watchChList), func(i, j int) {
		s.watchChList[i], s.watchChList[j] = s.watchChList[j], s.watchChList[i]
	})

	for _, w := range s.watchChList {
		if (w.prefix && strings.HasPrefix(taskKey, w.key)) || taskKey == w.key {
			response := &meta.WatchTaskResponse{
				Events: []meta.TaskEvent{
					{
						Tp:      eventTp,
						TaskID:  taskID,
						TaskKey: taskKey,
					},
				},
			}

			select {
			case w.ch <- response:
			default:
				go func() {
					w.ch <- response
				}()
			}
		}
	}
}

func (s *mockMetaStorage) WatchSupported() bool {
	return true
}

func (s *mockMetaStorage) WatchTask(ctx context.Context, key string, prefix bool) meta.WatchTaskChan {
	s.Lock()
	defer s.Unlock()
	ch := make(chan *meta.WatchTaskResponse)
	s.watchChList = append(s.watchChList, struct {
		ch     chan *meta.WatchTaskResponse
		key    string
		prefix bool
	}{ch: ch, key: key, prefix: prefix})

	go func() {
		select {
		case <-ctx.Done():
			s.Lock()
			defer s.Unlock()
			for i, w := range s.watchChList {
				if w.ch == ch {
					s.watchChList = append(s.watchChList[0:i], s.watchChList[i+1:]...)
				}
			}
			return
		}
	}()
	return ch
}

func (s *mockMetaStorage) CreateAgent(_ context.Context, agent *meta.AgentMeta) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.id2Agent[agent.ID]; ok {
		return meta.ErrDuplicatedID
	}

	s.id2Agent[agent.ID] = agent.Clone()
	return nil
}

func (s *mockMetaStorage) ListAgents(_ context.Context, id ...string) (agents []*meta.AgentMeta, err error) {
	s.Lock()
	defer s.Unlock()
	if len(id) > 0 {
		agents = make([]*meta.AgentMeta, 0, len(id))
		for _, agentID := range id {
			if agent, ok := s.id2Agent[agentID]; ok {
				agents = append(agents, agent.Clone())
			}
		}
	} else {
		agents = make([]*meta.AgentMeta, 0, len(s.id2Agent))
		for _, agent := range s.id2Agent {
			agents = append(agents, agent.Clone())
		}
	}
	return agents, nil
}

func (s *mockMetaStorage) DeleteAgent(_ context.Context, id string) (bool, error) {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.id2Agent[id]; ok {
		delete(s.id2Agent, id)
		return true, nil
	}
	return false, nil
}

func (s *mockMetaStorage) UpdateAgent(_ context.Context, id string, request *meta.AgentMetaUpdate) error {
	s.Lock()
	defer s.Unlock()
	agent, ok := s.id2Agent[id]
	if !ok {
		return meta.ErrNotFound
	}

	if tm := request.SetHeartbeatTime; tm != nil {
		agent.HeartbeatTime = *tm
	}

	if epochID := request.SetEpochID; epochID != "" {
		agent.EpochID = epochID
	}

	if status := request.SetStatus; status != "" {
		agent.Status = status
	}

	return nil
}

func (s *mockMetaStorage) filterTasks(f *meta.TaskFilter) (tasks []*meta.TaskMeta) {
	var tmpTasks []*meta.TaskMeta
	if len(f.ID) > 0 {
		tmpTasks = make([]*meta.TaskMeta, 0, len(f.ID))
		for id := range f.ID {
			if task, ok := s.id2Task[id]; ok {
				tmpTasks = append(tmpTasks, task)
			}
		}
	} else if f.Namespace != "" && f.Key != "" && !f.KeyPrefix {
		tmpTasks = make([]*meta.TaskMeta, 0, 1)
		if ns, ok := s.namespaces[f.Namespace]; ok {
			if task, ok := ns.key2Task[f.Key]; ok {
				tmpTasks = append(tmpTasks, task)
			}
		}
	}

	if tmpTasks == nil {
		tasks = make([]*meta.TaskMeta, 0, 16)
		for _, task := range s.id2Task {
			if !s.filterTask(task, f) {
				continue
			}

			tasks = append(tasks, task)
			if f.Limit > 0 && f.OrderBy == "" && len(tasks) == f.Limit {
				return tasks
			}
		}
	} else {
		tasks = tmpTasks[:0]
		for _, task := range tmpTasks {
			if !s.filterTask(task, f) {
				continue
			}

			tasks = append(tasks, task)
			if f.Limit > 0 && f.OrderBy == "" && len(tasks) == f.Limit {
				return tasks
			}
		}
	}

	switch f.OrderBy {
	case meta.TaskOrderByID:
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].ID < tasks[j].ID
		})
	case meta.TaskOrderByKey:
		sort.Slice(tasks, func(i, j int) bool {
			left, right := tasks[i], tasks[j]
			if left.Namespace == right.Namespace {
				return left.Key < right.Key
			}
			return left.Namespace < right.Namespace
		})
	}

	if f.Limit > 0 && f.Limit < len(tasks) {
		tasks = tasks[0:f.Limit]
	}

	return tasks
}

func (s *mockMetaStorage) filterTask(task *meta.TaskMeta, f *meta.TaskFilter) bool {
	if len(f.ID) > 0 {
		if _, ok := f.ID[task.ID]; !ok {
			return false
		}
	}

	if f.Namespace != "" && task.Namespace != f.Namespace {
		return false
	}

	if f.Key != "" {
		if f.KeyPrefix && !strings.HasPrefix(task.Key, f.Key) {
			return false
		}

		if !f.KeyPrefix && f.Key != task.Key {
			return false
		}
	}

	if f.Agent != nil && task.Agent != *f.Agent {
		return false
	}

	if f.Status != "" {
		status := task.Status
		if status == meta.TaskStatusRunning && f.ExpiredAgentTimeAsWaitStatus != nil {
			epoch := task.Agent
			if agent, ok := s.id2Agent[epoch.ID]; !ok || agent.AgentEpoch != epoch || agent.HeartbeatTime.Before(*f.ExpiredAgentTimeAsWaitStatus) {
				status = meta.TaskStatusWaiting
			}
		}

		if f.Status != status {
			return false
		}
	}

	return true
}

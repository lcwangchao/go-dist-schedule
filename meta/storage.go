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

package meta

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("not found")
var ErrDuplicatedID = errors.New("ID duplicated")
var ErrDuplicatedKey = errors.New("key duplicated")

type TaskMetaUpdate struct {
	SetAgent           *AgentEpoch
	SetCheckpoint      bool
	CheckPoint         []byte
	SetStatus          TaskStatus
	SetTerminateReason string
}

type AgentMetaUpdate struct {
	SetHeartbeatTime *time.Time
	SetEpochID       string
	SetStatus        AgentStatus
}

type TaskOrderBy string

const (
	TaskOrderByID  TaskOrderBy = "id"
	TaskOrderByKey             = "key"
)

type TaskFilter struct {
	ID                           map[string]struct{}
	Namespace                    string
	Key                          string
	KeyPrefix                    bool
	Agent                        *AgentEpoch
	Status                       TaskStatus
	ExpiredAgentTimeAsWaitStatus *time.Time
	Limit                        int
	OrderBy                      TaskOrderBy
}

type TaskEventTp int

const (
	TaskCreated TaskEventTp = iota
	TaskUpdated
	TaskDeleted
)

type TaskEvent struct {
	Tp      TaskEventTp
	TaskID  string
	TaskKey string
}

type WatchTaskResponse struct {
	Events []TaskEvent
}

type WatchTaskChan <-chan *WatchTaskResponse

type Storage interface {
	CreateTask(ctx context.Context, task *TaskMeta) error
	ListTasks(ctx context.Context, filter *TaskFilter) ([]*TaskMeta, error)
	DeleteTask(ctx context.Context, filter *TaskFilter) ([]string, error)
	UpdateTask(ctx context.Context, filter *TaskFilter, update *TaskMetaUpdate) ([]string, error)
	WatchTask(ctx context.Context, key string, prefix bool) WatchTaskChan
	WatchSupported() bool
	CreateAgent(ctx context.Context, agent *AgentMeta) error
	ListAgents(ctx context.Context, id ...string) ([]*AgentMeta, error)
	DeleteAgent(ctx context.Context, id string) (bool, error)
	UpdateAgent(ctx context.Context, id string, request *AgentMetaUpdate) error
}

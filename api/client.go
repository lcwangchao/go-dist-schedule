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

package api

import (
	"context"

	"github.com/lcwangchao/godistschedule/meta"
)

type SubmitTaskRequest struct {
	Namespace  string
	Key        string
	ExecutorID string
	Data       []byte
}

type TaskOp struct {
	meta.TaskFilter
}

type TaskOpFunc func(query *TaskOp)

func WithTaskID(taskID string, otherIDs ...string) TaskOpFunc {
	return func(query *TaskOp) {
		query.ID = map[string]struct{}{
			taskID: {},
		}

		if len(otherIDs) > 0 {
			for _, id := range otherIDs {
				query.ID[id] = struct{}{}
			}
		}
	}
}

func WithTaskKey(namespace, key string) TaskOpFunc {
	return func(query *TaskOp) {
		query.Namespace = namespace
		query.Key = key
		query.KeyPrefix = false
	}
}

func WithTaskKeyPrefix(namesapce, prefix string) TaskOpFunc {
	return func(query *TaskOp) {
		query.Key = prefix
		query.KeyPrefix = true
	}
}

func WithTaskStatus(status meta.TaskStatus) TaskOpFunc {
	return func(query *TaskOp) {
		query.Status = status
	}
}

func WithOrderByTaskID() TaskOpFunc {
	return func(query *TaskOp) {
		query.OrderBy = meta.TaskOrderByID
	}
}

func WithOrderByTaskKey() TaskOpFunc {
	return func(query *TaskOp) {
		query.OrderBy = meta.TaskOrderByKey
	}
}

type Client interface {
	SubmitTask(ctx context.Context, request *SubmitTaskRequest) (*meta.TaskMeta, error)
	ListTasks(ctx context.Context, ops ...TaskOpFunc) ([]*meta.TaskMeta, error)
	KillTasks(ctx context.Context, ops ...TaskOpFunc) ([]string, error)
	DeleteTasks(ctx context.Context, ops ...TaskOpFunc) ([]string, error)
	WatchSupported() bool
	WatchTasks(ctx context.Context, key string, isPrefix bool) meta.WatchTaskChan
	ListAgents(ctx context.Context) ([]*meta.AgentMeta, error)
}

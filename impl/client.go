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

	"github.com/google/uuid"

	"github.com/lcwangchao/godistschedule/api"
	"github.com/lcwangchao/godistschedule/meta"
)

type defaultClient struct {
	store meta.Storage
}

func NewDefaultClient(store meta.Storage) api.Client {
	return &defaultClient{
		store: store,
	}
}

func (c *defaultClient) SubmitTask(ctx context.Context, request *api.SubmitTaskRequest) (*meta.TaskMeta, error) {
	taskMeta := &meta.TaskMeta{
		ID:         uuid.NewString(),
		Namespace:  request.Namespace,
		Key:        request.Key,
		ExecutorID: request.ExecutorID,
		Data:       request.Data,
		Status:     meta.TaskStatusWaiting,
	}

	if taskMeta.Namespace == "" {
		taskMeta.Namespace = "default"
	}

	if err := c.store.CreateTask(ctx, taskMeta); err != nil {
		return nil, err
	}

	return taskMeta, nil
}

func (c *defaultClient) ListTasks(ctx context.Context, ops ...api.TaskOpFunc) ([]*meta.TaskMeta, error) {
	op := createTaskOp(ops)
	return c.store.ListTasks(ctx, &op.TaskFilter)
}

func (c *defaultClient) KillTasks(_ context.Context, _ ...api.TaskOpFunc) ([]string, error) {
	// TODO: support kill
	return nil, errors.New("not implemented")
}

func (c *defaultClient) DeleteTasks(ctx context.Context, ops ...api.TaskOpFunc) ([]string, error) {
	op := createTaskOp(ops)
	ids, err := c.store.DeleteTask(ctx, &op.TaskFilter)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (c *defaultClient) WatchSupported() bool {
	return c.store.WatchSupported()
}

func (c *defaultClient) WatchTasks(ctx context.Context, key string, isPrefix bool) meta.WatchTaskChan {
	return c.store.WatchTask(ctx, key, isPrefix)
}

func (c *defaultClient) ListAgents(ctx context.Context) ([]*meta.AgentMeta, error) {
	return c.store.ListAgents(ctx)
}

func createTaskOp(ops []api.TaskOpFunc) *api.TaskOp {
	taskOp := &api.TaskOp{}
	for _, op := range ops {
		op(taskOp)
	}
	return taskOp
}

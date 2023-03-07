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
	"github.com/lcwangchao/godistschedule/meta"
)

type CheckpointResult struct {
	Err error
}

func (r *CheckpointResult) Success() bool {
	return r.Err != nil
}

type TaskExecDriver interface {
	TaskID() string
	TaskMeta() *meta.TaskMeta
	Checkpoint(cp []byte) (done <-chan *CheckpointResult)
	ReleaseTask()
	TerminateTask(reason string)
	IsValid() bool
}

type EvictReason int

const (
	EvictReasonUnknown EvictReason = iota
	EvictReasonKill
)

type ExecutorContext struct {
	meta.AgentEpoch
	Client Client
}

type Executor interface {
	Init(ctx *ExecutorContext)
	ShouldLaunchTask(task *meta.TaskMeta) bool
	LaunchTask(task *meta.TaskMeta, driver TaskExecDriver)
	EvictTask(taskID string, reason EvictReason)
	Close()
}

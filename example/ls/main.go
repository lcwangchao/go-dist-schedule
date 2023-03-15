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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	schedule "github.com/lcwangchao/godistschedule"
	"github.com/lcwangchao/godistschedule/api"
	"github.com/lcwangchao/godistschedule/meta"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ListCmdData struct {
	Args []string `json:"args"`
}

type ListCmdExecutor struct {
	ctx    context.Context
	cancel func()
}

func newListCmdExecutor() api.Executor {
	return &ListCmdExecutor{}
}

func (e *ListCmdExecutor) LaunchTask(task *meta.TaskMeta, driver api.TaskExecDriver) {
	go func() {
		errorCode := ""
		defer func() {
			if errorCode != "" {
				driver.TerminateTask(meta.TaskTerminateError(errorCode))
			} else {
				driver.TerminateTask(meta.TaskTerminateDone)
			}
		}()

		var listCmdData ListCmdData
		if err := json.Unmarshal(task.Data, &listCmdData); err != nil {
			errorCode = "ParseError"
			log.Error("parse ls cmd error", zap.Error(err), zap.ByteString("data", task.Data))
			return
		}

		cmd := exec.Command("ls", listCmdData.Args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			errorCode = "CmdError"
			log.Error("exec ls error", zap.Error(err), zap.Strings("args", listCmdData.Args))
			return
		}

		fmt.Println("------------------------")
		fmt.Printf("> ls %s\n", strings.Join(listCmdData.Args, " "))
		fmt.Println(string(out))
	}()
}

func (e *ListCmdExecutor) ShouldLaunchTask(_ *meta.TaskMeta) bool { return true }

func (e *ListCmdExecutor) Init(_ *api.ExecutorContext) {}

func (e *ListCmdExecutor) EvictTask(_ string, _ api.EvictReason) {}

func (e *ListCmdExecutor) Close() {}

func main() {
	store := schedule.NewMockMetaStorage()
	agent := schedule.NewNodeAgent("agent1", store)
	if err := agent.RegisterExecutor("ls", newListCmdExecutor); err != nil {
		panic(err)
	}
	agent.Start()

	cli := schedule.NewClient(store)
	task, err := cli.SubmitTask(context.TODO(), &api.SubmitTaskRequest{
		Key:        fmt.Sprintf("/ls/%d", time.Now().UnixMilli()),
		ExecutorID: "ls",
		Data:       []byte(`{"args": ["/"]}`),
	})
	if err != nil {
		panic(err)
	}

	for {
		tasks, err := cli.ListTasks(context.TODO(), api.WithTaskID(task.ID))
		if err != nil {
			panic(err)
		}

		if len(tasks) == 0 {
			panic("task not found")
		}

		found := tasks[0]
		if found.Status != meta.TaskStatusTerminated {
			time.Sleep(time.Second)
			continue
		}

		log.Info("task terminated", zap.String("reason", found.TerminateReason))
		return
	}
}

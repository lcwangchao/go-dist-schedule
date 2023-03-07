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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	schedule "github.com/lcwangchao/godistschedule"
	"github.com/lcwangchao/godistschedule/api"
	"github.com/lcwangchao/godistschedule/meta"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func prepareResources() (string, []string) {
	dir, err := os.MkdirTemp(os.TempDir(), "wcfiles-")
	if err != nil {
		panic(err)
	}

	fileCnt := 10
	filenames := make([]string, 0, fileCnt)
	for i := 1; i <= fileCnt; i++ {
		fileName := fmt.Sprintf("file%02d.txt", i)
		filePath := filepath.Join(dir, fileName)
		if err = os.WriteFile(filePath, []byte(fmt.Sprintf("This is the content of the index idx-%d\n", i)), 0666); err != nil {
			panic(err)
		}
		filenames = append(filenames, fileName)
	}

	log.Info("resource prepared", zap.String("dir", dir), zap.Strings("filenames", filenames))
	return dir, filenames
}

func startAgents(store meta.Storage) []api.Agent {
	newAgent := func() api.Agent {
		agent := schedule.NewNodeAgent(uuid.NewString(), store)
		if err := agent.RegisterExecutor(coordinateExecutorID, newCoordinateExecutor); err != nil {
			panic(err)
		}

		if err := agent.RegisterExecutor(mapExecutorID, newMapExecutor); err != nil {
			panic(err)
		}

		if err := agent.RegisterExecutor(reduceExecutorID, newReduceExecutor); err != nil {
			panic(err)
		}
		return agent
	}

	agents := make([]api.Agent, 4)
	for i := range agents {
		agents[i] = newAgent()
		agents[i].Start()
	}
	return agents
}

func runWordCount(cli api.Client, folder string) {
	task, err := cli.SubmitTask(context.TODO(), &api.SubmitTaskRequest{
		Key:        fmt.Sprintf("wc-main-%d", time.Now().Unix()),
		ExecutorID: coordinateExecutorID,
		Data:       []byte(folder),
	})

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	checkResult := func() bool {
		tasks, err := cli.ListTasks(ctx, api.WithTaskID(task.ID))
		if err != nil {
			log.Error("failed to get task", zap.Error(err))
			return false
		}

		if len(tasks) == 0 {
			log.Error("task is deleted")
			return true
		}

		task = tasks[0]
		if task.Status == meta.TaskStatusTerminated {
			log.Info("task terminated", zap.String("reason", task.TerminateReason))
			if task.TerminateReason == meta.TaskTerminateDone {
				resultFile := filepath.Join(folder, "result.out")
				result := make(map[string]int64)
				if err = readFileJson(resultFile, &result); err != nil {
					panic(err)
				}

				fmt.Println("===== Result =====")
				for w, n := range result {
					fmt.Printf("%s: %d\n", w, n)
				}
			}
			return true
		}
		return false
	}

	ticker := time.Tick(10 * time.Second)
	watcher := cli.WatchTasks(ctx, task.Key, false)
	for {
		select {
		case <-ctx.Done():
			log.Error("context is done", zap.Error(err))
		case <-watcher:
			if checkResult() {
				return
			}
		case <-ticker:
			if checkResult() {
				return
			}
			log.Info("task is running", zap.String("status", string(task.Status)))
		}
	}
}

func main() {
	store := schedule.NewMockMetaStorage()
	dir, _ := prepareResources()
	startAgents(store)
	time.Sleep(time.Second)
	runWordCount(schedule.NewClient(store), dir)
}

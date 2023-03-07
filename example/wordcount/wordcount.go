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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lcwangchao/godistschedule/api"
	"github.com/lcwangchao/godistschedule/meta"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type simpleExecutor struct {
	sync.Mutex
	agent     meta.AgentEpoch
	ctx       context.Context
	cli       api.Client
	cancel    func()
	closed    bool
	stopTasks map[string]func()
	runTask   func(context.Context, *meta.TaskMeta, api.TaskExecDriver) error
}

func (e *simpleExecutor) Init(executorCtx *api.ExecutorContext) {
	e.agent = executorCtx.AgentEpoch
	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.stopTasks = make(map[string]func())
	e.cli = executorCtx.Client
}

func (e *simpleExecutor) ShouldLaunchTask(_ *meta.TaskMeta) bool {
	e.Lock()
	defer e.Unlock()
	return !e.closed
}

func (e *simpleExecutor) LaunchTask(task *meta.TaskMeta, driver api.TaskExecDriver) {
	e.Lock()
	defer e.Unlock()
	if e.closed {
		return
	}
	log.Info("launch task", zap.String("agent", e.agent.ID), zap.String("taskID", task.ID))
	ctx, cancel := context.WithCancel(e.ctx)
	e.stopTasks[task.ID] = cancel
	go func() {
		if err := e.runTask(ctx, task, driver); err != nil {
			if err != ctx.Err() {
				log.Error("error occurs", zap.Error(err))
				driver.TerminateTask("")
			} else {
				driver.ReleaseTask()
			}
		} else {
			driver.TerminateTask(meta.TaskTerminateDone)
		}
	}()
}

func (e *simpleExecutor) EvictTask(taskID string, _ api.EvictReason) {
	e.Lock()
	defer e.Unlock()
	if e.closed {
		return
	}

	if stop, ok := e.stopTasks[taskID]; ok {
		stop()
		delete(e.stopTasks, taskID)
	}
}

func (e *simpleExecutor) Close() {
	e.Lock()
	defer e.Unlock()
	if e.closed {
		return
	}

	e.cancel()
	e.stopTasks = nil
	e.closed = true
}

const (
	coordinateExecutorID = "wc-coord"
	mapExecutorID        = "wc-map"
	reduceExecutorID     = "wc-reduce"
)

const (
	stepInit   = "init"
	stepMap    = "map"
	stepReduce = "reduce"
	stepResult = "result"
)

type checkpoint struct {
	step   string
	files  []string
	result map[string]int64
}

func (c *checkpoint) serialize() ([]byte, error) {
	return json.Marshal(c)
}

func loadCheckPoint(data []byte) (*checkpoint, error) {
	if len(data) == 0 {
		return &checkpoint{
			step: stepInit,
		}, nil
	}

	var cp *checkpoint
	if err := json.Unmarshal(data, cp); err != nil {
		return nil, err
	}
	return cp, nil
}

type coordinateExecutor struct {
	simpleExecutor
}

func newCoordinateExecutor() api.Executor {
	exec := &coordinateExecutor{}
	exec.simpleExecutor.runTask = exec.doRunTask
	return exec
}

func (e *coordinateExecutor) runStepInit(folder string, cp *checkpoint) (*checkpoint, error) {
	files, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}

	fileNames := make([]string, 0, len(files))
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".txt") {
			continue
		}
		fileNames = append(fileNames, file.Name())
	}

	cp = &checkpoint{
		step:  stepMap,
		files: fileNames,
	}

	if len(fileNames) == 0 {
		cp.step = stepResult
	}

	return cp, nil
}

func (e *coordinateExecutor) runStepMap(ctx context.Context, task *meta.TaskMeta, folder string, cp *checkpoint) (*checkpoint, error) {
	for _, fileName := range cp.files {
		time.Sleep(100 * time.Millisecond)
		_, err := e.cli.SubmitTask(ctx, &api.SubmitTaskRequest{
			Key:        task.Key + "/mapper/" + fileName,
			ExecutorID: mapExecutorID,
			Data:       []byte(filepath.Join(folder, fileName)),
		})

		if err == meta.ErrDuplicatedKey {
			continue
		}

		if err != nil {
			return nil, err
		}
	}

	if err := e.waitTasksDone(ctx, task.Key+"/mapper/"); err != nil {
		return nil, err
	}

	cp.step = stepReduce
	return cp, nil
}

func (e *coordinateExecutor) runStepReduce(ctx context.Context, task *meta.TaskMeta, folder string, cp *checkpoint) (*checkpoint, error) {
	_, err := e.cli.SubmitTask(e.ctx, &api.SubmitTaskRequest{
		Key:        task.Key + "/reducer",
		ExecutorID: reduceExecutorID,
		Data:       []byte(folder),
	})

	if err != nil {
		return nil, err
	}

	if err = e.waitTasksDone(ctx, task.Key+"/reducer"); err != nil {
		return nil, err
	}

	cp.step = stepResult
	return cp, nil
}

func (e *coordinateExecutor) runStepResult(_ context.Context, folder string, cp *checkpoint) (*checkpoint, error) {
	resultFile := filepath.Join(folder, "result.out")
	err := readFileJson(resultFile, &cp.result)
	if err != nil {
		return nil, err
	}
	return cp, nil
}

func (e *coordinateExecutor) doRunTask(ctx context.Context, task *meta.TaskMeta, driver api.TaskExecDriver) error {
	folder := string(task.Data)
	cp, err := loadCheckPoint(task.CheckPoint)
	if err != nil {
		return err
	}

	for cp.step != stepResult {
		switch cp.step {
		case stepInit:
			log.Info("wc step init")
			cp, err = e.runStepInit(folder, cp)
		case stepMap:
			log.Info("wc step map")
			cp, err = e.runStepMap(ctx, task, folder, cp)
		case stepReduce:
			log.Info("wc step reduce")
			cp, err = e.runStepReduce(ctx, task, folder, cp)
		case stepResult:
			log.Info("wc step result")
			cp, err = e.runStepResult(ctx, folder, cp)
		}

		if err != nil {
			return err
		}

		if err = e.checkpoint(driver, cp); err != nil {
			return err
		}

		if cp.step == stepResult {
			break
		}
	}

	return nil
}

func (e *coordinateExecutor) checkpoint(driver api.TaskExecDriver, cp *checkpoint) error {
	cpData, err := cp.serialize()
	if err != nil {
		return err
	}

	select {
	case <-e.ctx.Done():
		return e.ctx.Err()
	case result := <-driver.Checkpoint(cpData):
		return result.Err
	}
}

func (e *coordinateExecutor) waitTasksDone(ctx context.Context, prefix string) error {
	for ctx.Err() == nil {
		time.Sleep(2 * time.Second)
		tasks, err := e.cli.ListTasks(ctx, api.WithTaskKeyPrefix("", prefix))
		if err != nil {
			return err
		}

		completed := true
		for _, task := range tasks {
			if task.Status != meta.TaskStatusTerminated {
				completed = false
				continue
			}

			if task.TerminateReason != meta.TaskTerminateDone {
				return errors.New("map task error")
			}
		}

		if completed {
			return nil
		}
	}

	return ctx.Err()
}

type mapExecutor struct {
	simpleExecutor
}

func newMapExecutor() api.Executor {
	exec := &mapExecutor{}
	exec.simpleExecutor.runTask = exec.doRunTask
	return exec
}

func (e *mapExecutor) doRunTask(_ context.Context, task *meta.TaskMeta, _ api.TaskExecDriver) error {
	filePath := string(task.Data)
	log.Info("mapExecutor run", zap.String("path", filePath))
	bs, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	words := make(map[string]int64)
	for _, w := range strings.Fields(string(bs)) {
		incMapValue(words, w, 1)
	}
	return writeFileJson(filePath+".out", words)
}

type reduceExecutor struct {
	simpleExecutor
}

func newReduceExecutor() api.Executor {
	exec := &reduceExecutor{}
	exec.simpleExecutor.runTask = exec.doRunTask
	return exec
}

func (e *reduceExecutor) doRunTask(_ context.Context, task *meta.TaskMeta, _ api.TaskExecDriver) error {
	folder := string(task.Data)
	log.Info("reduceExecutor run", zap.String("folder", folder))
	files, err := os.ReadDir(folder)
	if err != nil {
		return err
	}

	result := make(map[string]int64)
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".txt.out") {
			continue
		}

		fileResult := make(map[string]int64)
		if err = readFileJson(filepath.Join(folder, f.Name()), &fileResult); err != nil {
			return err
		}

		for w, n := range fileResult {
			incMapValue(result, w, n)
		}
	}
	return writeFileJson(filepath.Join(folder, "result.out"), result)
}

func incMapValue(m map[string]int64, key string, n int64) {
	if v, ok := m[key]; ok {
		m[key] = v + n
	} else {
		m[key] = n
	}
}

func readFileJson(path string, v any) error {
	bs, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(bs, v)
}

func writeFileJson(path string, v any) error {
	bs, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, bs, 0666)
}

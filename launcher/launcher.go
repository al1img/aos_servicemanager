// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2021 Renesas Electronics Corporation.
// Copyright (C) 2021 EPAM Systems, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package launcher

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_common/utils/action"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_servicemanager/config"
	"github.com/aoscloud/aos_servicemanager/runner"
	"github.com/aoscloud/aos_servicemanager/servicemanager"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const maxParallelInstanceActions = 32

const runtimeDir = "/run/aos/runtime"

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Storage storage interface.
type Storage interface {
	AddInstance(instance InstanceInfo) error
	UpdateInstance(instance InstanceInfo) error
	RemoveInstance(instanceID string) error
	GetInstanceByIndex(serviceID, subjectID string, index uint64) (InstanceInfo, error)
	GetRunningInstances() ([]InstanceInfo, error)
	GetSubjectInstances(subjectID string) ([]InstanceInfo, error)
}

// ServiceProvider service provider.
type ServiceProvider interface {
	GetServiceInfo(serviceID string) (servicemanager.ServiceInfo, error)
}

// InstanceRunner interface to start/stop service instances.
type InstanceRunner interface {
	StartInstance(instanceID, runtimeDir string, params runner.StartInstanceParams) runner.InstanceStatus
	StopInstance(instanceID string) error
	InstanceStatusChannel() <-chan []runner.InstanceStatus
}

// InstanceInfo instance information.
type InstanceInfo struct {
	ServiceID   string
	SubjectID   string
	InstanceID  string
	Index       uint64
	UnitSubject bool
	Running     bool
}

// RuntimeStatus runtime status info.
type RuntimeStatus struct {
	RunStatus    *RunInstancesStatus
	UpdateStatus *UpdateInstancesStatus
}

// RunInstancesStatus run instances status.
type RunInstancesStatus struct {
	UnitSubjects  []string
	Instances     []cloudprotocol.InstanceStatus
	ErrorServices []cloudprotocol.ServiceStatus
}

// UpdateInstancesStatus update instances status.
type UpdateInstancesStatus struct {
	Instances []cloudprotocol.InstanceStatus
}

// Launcher launcher instance.
type Launcher struct {
	sync.Mutex

	storage         Storage
	serviceProvider ServiceProvider
	instanceRunner  InstanceRunner

	currentSubjects        []string
	runtimeStatusChannel   chan RuntimeStatus
	cancelFunction         context.CancelFunc
	actionHandler          *action.Handler
	runMutex               sync.RWMutex
	runInstancesInProgress bool
	currentInstances       map[string]*instanceInfo
	currentServices        map[string]*serviceInfo
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	// ErrNotExist not exist instance error.
	ErrNotExist = errors.New("instance not exist")
	// ErrNoRuntimeStatus no current runtime status error.
	ErrNoRuntimeStatus = errors.New("no runtime status")
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new launcher object.
func New(config *config.Config, storage Storage, serviceProvider ServiceProvider,
	instanceRunner InstanceRunner,
) (launcher *Launcher, err error) {
	log.WithField("runner", config.Runner).Debug("New launcher")

	launcher = &Launcher{
		storage: storage, serviceProvider: serviceProvider, instanceRunner: instanceRunner,

		actionHandler:        action.New(maxParallelInstanceActions),
		runtimeStatusChannel: make(chan RuntimeStatus, 1),
	}

	ctx, cancelFunction := context.WithCancel(context.Background())

	launcher.cancelFunction = cancelFunction

	go launcher.handleRunnerStatus(ctx)

	// TODO: remove not needed DB entries

	return launcher, nil
}

// Close closes launcher.
func (launcher *Launcher) Close() (err error) {
	launcher.Lock()
	defer launcher.Unlock()

	log.Debug("Close launcher")

	launcher.cancelFunction()

	launcher.stopInstances(nil)

	return err
}

// SendCurrentRuntimeStatus forces launcher to send current runtime status.
func (launcher *Launcher) SendCurrentRuntimeStatus() error {
	launcher.Lock()
	defer launcher.Unlock()

	launcher.runMutex.Lock()
	defer launcher.runMutex.Unlock()

	// Send current run status only if it is available
	if launcher.currentInstances == nil {
		return ErrNoRuntimeStatus
	}

	launcher.sendRunInstancesStatuses()

	return nil
}

// SubjectsChanged notifies launcher that subjects are changed.
func (launcher *Launcher) SubjectsChanged(subjects []string) error {
	launcher.Lock()
	defer launcher.Unlock()

	if isSubjectsEqual(launcher.currentSubjects, subjects) {
		return nil
	}

	log.WithField("subjects", subjects).Info("Subjects changed")

	launcher.currentSubjects = subjects

	instances, err := launcher.storage.GetRunningInstances()
	if err != nil {
		log.Errorf("Can't get running instances: %s", err)

		// Try to get at least currently running instances
		for _, currentInstance := range launcher.currentInstances {
			instances = append(instances, currentInstance.InstanceInfo)
		}
	}

	// Remove unit subjects instances
	i := 0

	for _, instance := range instances {
		if !instance.UnitSubject {
			instances[i] = instance
			i++
		}
	}

	instances = instances[:i]

	for _, subject := range subjects {
		subjectInstances, err := launcher.storage.GetSubjectInstances(subject)
		if err != nil {
			log.WithField("subject", subject).Errorf("Can't get subject instances: %s", err)
			continue
		}

		instances = append(instances, subjectInstances...)
	}

	launcher.runInstances(instances)

	return nil
}

// RunInstances runs desired services instances.
func (launcher *Launcher) RunInstances(instances []cloudprotocol.InstanceInfo) error {
	launcher.Lock()
	defer launcher.Unlock()

	log.Debug("Run instances")

	runInstances := make([]InstanceInfo, 0, len(instances))

	// Convert cloudprotocol InstanceInfo to internal InstanceInfo
	for _, item := range instances {
		for i := uint64(0); i < item.NumInstances; i++ {
			// Get instance from current map. If not available, get it from storage. Otherwise, generate new instance.
			instance, err := launcher.getCurrentInstanceByIndex(item.ServiceID, item.SubjectID, i)
			if err != nil {
				if instance, err = launcher.storage.GetInstanceByIndex(
					item.ServiceID, item.SubjectID, i); err != nil {
					if instance, err = launcher.createNewInstance(item.ServiceID, item.SubjectID, i); err != nil {
						log.WithFields(instance.logFields()).Errorf("Can't create instance: %s", err)
					}
				}
			}

			runInstances = append(runInstances, instance)
		}
	}

	launcher.runInstances(runInstances)

	return nil
}

// StopAllInstances stops all running instances.
func (launcher *Launcher) StopAllInstances() error {
	launcher.Lock()
	defer launcher.Unlock()

	launcher.currentSubjects = nil

	launcher.stopInstances(nil)

	return nil
}

// OverrideEnvVars overrides service instance environment variables.
func (launcher *Launcher) OverrideEnvVars(
	envVarsInfo []cloudprotocol.EnvVarsInstanceInfo,
) ([]cloudprotocol.EnvVarsInstanceStatus, error) {
	launcher.Lock()
	defer launcher.Unlock()

	return nil, nil
}

// RuntimeStatusChannel returns runtime status channel.
func (launcher *Launcher) RuntimeStatusChannel() <-chan RuntimeStatus {
	return launcher.runtimeStatusChannel
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (launcher *Launcher) handleRunnerStatus(ctx context.Context) {
	for {
		select {
		case instances := <-launcher.instanceRunner.InstanceStatusChannel():
			launcher.updateInstancesStatuses(instances)

		case <-ctx.Done():
			return
		}
	}
}

func (launcher *Launcher) updateInstancesStatuses(instances []runner.InstanceStatus) {
	launcher.runMutex.Lock()
	defer launcher.runMutex.Unlock()

	updateInstancesStatus := &UpdateInstancesStatus{Instances: make([]cloudprotocol.InstanceStatus, 0, len(instances))}

	for _, instanceStatus := range instances {
		currentInstance, ok := launcher.currentInstances[instanceStatus.InstanceID]
		if !ok {
			log.WithField("instanceID", instanceStatus.InstanceID).Warn("Not running instance status received")
			continue
		}

		if currentInstance.runStatus != instanceStatus {
			currentInstance.runStatus = instanceStatus
			if !launcher.runInstancesInProgress {
				updateInstancesStatus.Instances = append(updateInstancesStatus.Instances, currentInstance.cloudStatus())
			}
		}

		switch instanceStatus.State {
		case cloudprotocol.InstanceStateActive:
			log.WithFields(currentInstance.logFields()).Info("Instance successfully started")

		case cloudprotocol.InstanceStateFailed:
			log.WithFields(currentInstance.logFields()).Errorf("Instance failed: %s", currentInstance.runStatus.Err)
		}
	}

	if len(updateInstancesStatus.Instances) > 0 {
		launcher.runtimeStatusChannel <- RuntimeStatus{UpdateStatus: updateInstancesStatus}
	}
}

func (launcher *Launcher) createNewInstance(serviceID, subjectID string, index uint64) (InstanceInfo, error) {
	instance := InstanceInfo{
		ServiceID:   serviceID,
		SubjectID:   subjectID,
		InstanceID:  uuid.New().String(),
		Index:       index,
		UnitSubject: launcher.isCurrentSubject(subjectID),
	}

	if err := launcher.storage.AddInstance(instance); err != nil {
		return instance, aoserrors.Wrap(err)
	}

	return instance, nil
}

func (launcher *Launcher) runInstances(instances []InstanceInfo) {
	launcher.runMutex.Lock()

	launcher.runInstancesInProgress = true

	if launcher.currentInstances == nil {
		launcher.currentInstances = make(map[string]*instanceInfo)
	}

	launcher.runMutex.Unlock()

	defer func() {
		launcher.runMutex.Lock()
		launcher.runInstancesInProgress = false
		launcher.sendRunInstancesStatuses()
		launcher.runMutex.Unlock()
	}()

	launcher.stopInstances(instances)
	launcher.startInstances(instances)
}

func (launcher *Launcher) stopInstances(instances []InstanceInfo) {
	launcher.runMutex.RLock()

stopLoop:
	for _, currentInstance := range launcher.currentInstances {
		for _, instance := range instances {
			if instance.InstanceID == currentInstance.InstanceID {
				continue stopLoop
			}
		}

		launcher.doStopAction(currentInstance)
	}

	launcher.runMutex.RUnlock()

	launcher.actionHandler.Wait()
}

func (launcher *Launcher) doStopAction(instance *instanceInfo) {
	launcher.actionHandler.Execute(instance.InstanceID, func(instanceID string) (err error) {
		defer launcher.removeInstanceFromCurrentList(instance.InstanceID)

		if err = launcher.stopInstance(instance); err != nil {
			log.WithFields(instance.logFields()).Errorf("Can't stop instance: %s", err)

			return aoserrors.Wrap(err)
		}

		log.WithFields(instance.logFields()).Info("Instance successfully stopped")

		return nil
	})
}

func (launcher *Launcher) stopInstance(instance *instanceInfo) (err error) {
	log.WithFields(instance.logFields()).Debug("Stop instance")

	instance.Running = false

	if storageErr := launcher.storage.UpdateInstance(instance.InstanceInfo); storageErr != nil {
		if err == nil {
			err = aoserrors.Wrap(storageErr)
		}
	}

	if runnerErr := launcher.instanceRunner.StopInstance(instance.InstanceID); runnerErr != nil {
		if err == nil {
			err = runnerErr
		}
	}

	return err
}

func (launcher *Launcher) startInstances(instances []InstanceInfo) {
	// Cache services info to do not read them each time from storage
	launcher.currentServices = launcher.getCurrentServices(instances)

	launcher.runMutex.RLock()

	for _, instance := range instances {
		restartInstance := false

		if currentInstance, ok := launcher.currentInstances[instance.InstanceID]; ok {
			// If running instance has the same service version no need to restart this instance
			if currentInstance.serviceVersion == launcher.currentServices[currentInstance.ServiceID].AosVersion {
				continue
			}

			restartInstance = true
		}

		launcher.doStartAction(instance, restartInstance)
	}

	launcher.runMutex.RUnlock()

	launcher.actionHandler.Wait()
}

func (launcher *Launcher) doStartAction(instance InstanceInfo, restart bool) {
	launcher.actionHandler.Execute(instance.InstanceID, func(instanceID string) (err error) {
		// it is safe to modify runInstance fields in different threads as far as each thread modifies each own
		// instance. Reading these fields to create cloud status (by calling cloudStatus())
		// Except runStatus, which should be guarded because can be modified on instances statuses update
		// from runner.
		runInstance := launcher.addInstanceToCurrentList(instance)

		defer func() {
			if err != nil {
				log.WithFields(instance.logFields()).Errorf("Instance failed: %s", err)

				// Update current status if it is not updated before.
				launcher.runMutex.Lock()
				if runInstance.runStatus.State == "" {
					runInstance.runStatus.State = cloudprotocol.InstanceStateFailed
					runInstance.runStatus.Err = err
				}
				launcher.runMutex.Unlock()

				return
			}

			log.WithFields(instance.logFields()).Info("Instance successfully started")
		}()

		if restart {
			if err = launcher.stopInstance(runInstance); err != nil {
				log.WithFields(instance.logFields()).Errorf("Can't stop instance: %s", err)
			}
		}

		if err = launcher.startInstance(runInstance); err != nil {
			return aoserrors.Wrap(err)
		}

		return nil
	})
}

func (launcher *Launcher) startInstance(instance *instanceInfo) error {
	log.WithFields(instance.logFields()).Debug("Start instance")

	service, err := launcher.getCurrentServiceInfo(instance.ServiceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	instance.serviceVersion = service.AosVersion
	instance.Running = true

	if err := launcher.storage.UpdateInstance(instance.InstanceInfo); err != nil {
		return aoserrors.Wrap(err)
	}

	runStatus := launcher.instanceRunner.StartInstance(
		instance.InstanceID, filepath.Join(runtimeDir, instance.InstanceID), runner.StartInstanceParams{})

	// Update current status if it is not updated by runner status channel. Instance runner status goes asynchronously
	// by status channel. And therefore, new status may arrive before returning by StartInstance API. We detect this
	// situation by checking if run state is not empty value.
	launcher.runMutex.Lock()
	defer launcher.runMutex.Unlock()

	if instance.runStatus.State == "" {
		instance.runStatus = runStatus
	}

	if instance.runStatus.State == cloudprotocol.InstanceStateFailed {
		return aoserrors.Wrap(instance.runStatus.Err)
	}

	return nil
}

func (launcher *Launcher) sendRunInstancesStatuses() {
	runInstancesStatuses := make([]cloudprotocol.InstanceStatus, 0, len(launcher.currentInstances))

	for _, currentInstance := range launcher.currentInstances {
		runInstancesStatuses = append(runInstancesStatuses, currentInstance.cloudStatus())
	}

	launcher.runtimeStatusChannel <- RuntimeStatus{
		RunStatus: &RunInstancesStatus{
			UnitSubjects: launcher.currentSubjects,
			Instances:    runInstancesStatuses,
		},
	}
}

func (launcher *Launcher) isCurrentSubject(subject string) bool {
	for _, currentSubject := range launcher.currentSubjects {
		if subject == currentSubject {
			return true
		}
	}

	return false
}

func isSubjectsEqual(subjects1, subjects2 []string) bool {
	if subjects1 == nil && subjects2 == nil {
		return true
	}

	if subjects1 == nil || subjects2 == nil {
		return false
	}

	if len(subjects1) != len(subjects2) {
		return false
	}

	for i := range subjects1 {
		if subjects1[i] != subjects2[i] {
			return false
		}
	}

	return true
}

// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Renesas Electronics Corporation.
// Copyright (C) 2022 EPAM Systems, Inc.
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

package launcher_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_servicemanager/config"
	"github.com/aoscloud/aos_servicemanager/launcher"
	"github.com/aoscloud/aos_servicemanager/runner"
	"github.com/aoscloud/aos_servicemanager/servicemanager"
)

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testStorage struct {
	sync.RWMutex
	instances map[string]launcher.InstanceInfo
}

type testServiceProvider struct {
	services map[string]servicemanager.ServiceInfo
}

type testRunner struct {
	sync.Mutex
	statusChannel chan []runner.InstanceStatus
	startFunc     func(instanceID string) runner.InstanceStatus
	stopFunc      func(instanceID string) error
}

type testInstance struct {
	serviceID      string
	serviceVersion uint64
	subjectID      string
	numInstances   uint64
	unitSubject    bool
	err            error
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var tmpDir string

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Fatalf("Setup error: %s", err)
	}

	ret := m.Run()

	if err := cleanup(); err != nil {
		log.Errorf("Cleanup error: %s", err)
	}

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestRunInstances(t *testing.T) {
	type testData struct {
		instances []testInstance
	}

	data := []testData{
		// start from scretch
		{
			instances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject2", numInstances: 2},
			},
		},
		// start the same instances
		{
			instances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject2", numInstances: 2},
			},
		},
		// stop and start some instances
		{
			instances: []testInstance{
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject2", numInstances: 2},
				{serviceID: "service2", serviceVersion: 2, subjectID: "subject1", numInstances: 3},
				{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 4},
			},
		},
		// new service version
		{
			instances: []testInstance{
				{serviceID: "service1", serviceVersion: 2, subjectID: "subject1", numInstances: 1},
				{serviceID: "service1", serviceVersion: 2, subjectID: "subject2", numInstances: 2},
				{serviceID: "service2", serviceVersion: 3, subjectID: "subject1", numInstances: 3},
				{serviceID: "service2", serviceVersion: 3, subjectID: "subject2", numInstances: 4},
			},
		},
		// start error
		{
			instances: []testInstance{
				{
					serviceID: "service3", serviceVersion: 3, subjectID: "subject3", numInstances: 3,
					err: errors.New("some error"), // nolint:goerr113
				},
			},
		},
		// stop all instances
		{
			instances: []testInstance{},
		},
	}

	var currentTestInstaces []testInstance

	runningInstances := make(map[string]runner.InstanceStatus)

	storage := newTestStorage()
	serviceProvider := newTestServiceProvider()
	instanceRunner := newTestRunner(
		func(instanceID string) runner.InstanceStatus {
			status := getRunnerStatus(instanceID, currentTestInstaces, storage)
			runningInstances[instanceID] = status

			return status
		},
		func(instanceID string) error {
			delete(runningInstances, instanceID)

			return nil
		},
	)

	testLauncher, err := launcher.New(&config.Config{WorkingDir: tmpDir}, storage, serviceProvider, instanceRunner)
	if err != nil {
		t.Fatalf("Can't create launcher: %s", err)
	}
	defer testLauncher.Close()

	for i, item := range data {
		t.Logf("Run instances: %d", i)

		currentTestInstaces = item.instances

		if err = serviceProvider.fromTestInstances(currentTestInstaces); err != nil {
			t.Fatalf("Can't create test services: %s", err)
		}

		if err = testLauncher.RunInstances(createInstancesInfos(currentTestInstaces)); err != nil {
			t.Fatalf("Can't run instances: %s", err)
		}

		runtimeStatus := launcher.RuntimeStatus{
			RunStatus: &launcher.RunInstancesStatus{Instances: createInstancesStatuses(currentTestInstaces)},
		}

		if err = checkRuntimeStatus(runtimeStatus, testLauncher.RuntimeStatusChannel()); err != nil {
			t.Errorf("Check runtime status error: %s", err)
		}

		if len(runtimeStatus.RunStatus.Instances) != len(runningInstances) {
			t.Errorf("Wrong running instances count: %d", len(runningInstances))
		}
	}
}

func TestUpdateInstances(t *testing.T) {
	storage := newTestStorage()
	serviceProvider := newTestServiceProvider()
	instanceRunner := newTestRunner(nil, nil)

	testLauncher, err := launcher.New(&config.Config{WorkingDir: tmpDir}, storage, serviceProvider, instanceRunner)
	if err != nil {
		t.Fatalf("Can't create launcher: %s", err)
	}
	defer testLauncher.Close()

	testInstances := []testInstance{
		{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 1},
		{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 2},
		{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 3},
	}

	if err = serviceProvider.fromTestInstances(testInstances); err != nil {
		t.Fatalf("Can't create test services: %s", err)
	}

	if err = testLauncher.RunInstances(createInstancesInfos(testInstances)); err != nil {
		t.Fatalf("Can't run instances: %s", err)
	}

	runtimeStatus := launcher.RuntimeStatus{
		RunStatus: &launcher.RunInstancesStatus{Instances: createInstancesStatuses(testInstances)},
	}

	if err = checkRuntimeStatus(runtimeStatus, testLauncher.RuntimeStatusChannel()); err != nil {
		t.Errorf("Check runtime status error: %s", err)
	}

	changedInstances := []testInstance{
		{
			serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 2,
			err: errors.New("some error"), // nolint:goerr113
		},
	}

	runStatus, err := createRunStatus(storage, changedInstances)
	if err != nil {
		t.Fatalf("Can't create run status: %s", err)
	}

	instanceRunner.statusChannel <- runStatus

	runtimeStatus = launcher.RuntimeStatus{
		UpdateStatus: &launcher.UpdateInstancesStatus{Instances: createInstancesStatuses(changedInstances)},
	}

	if err = checkRuntimeStatus(runtimeStatus, testLauncher.RuntimeStatusChannel()); err != nil {
		t.Errorf("Check runtime status error: %s", err)
	}
}

func TestSendCurrentRuntimeStatus(t *testing.T) {
	serviceProvider := newTestServiceProvider()

	testLauncher, err := launcher.New(&config.Config{WorkingDir: tmpDir}, newTestStorage(), serviceProvider,
		newTestRunner(nil, nil))
	if err != nil {
		t.Fatalf("Can't create launcher: %s", err)
	}
	defer testLauncher.Close()

	testInstances := []testInstance{
		{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 1},
		{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 2},
		{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 3},
	}

	if err = serviceProvider.fromTestInstances(testInstances); err != nil {
		t.Fatalf("Can't create test services: %s", err)
	}

	if err = testLauncher.SendCurrentRuntimeStatus(); !errors.Is(launcher.ErrNoRuntimeStatus, err) {
		t.Error("No runtime status error expected")
	}

	if err = testLauncher.RunInstances(createInstancesInfos(testInstances)); err != nil {
		t.Fatalf("Can't run instances: %s", err)
	}

	runtimeStatus := launcher.RuntimeStatus{
		RunStatus: &launcher.RunInstancesStatus{Instances: createInstancesStatuses(testInstances)},
	}

	if err = checkRuntimeStatus(runtimeStatus, testLauncher.RuntimeStatusChannel()); err != nil {
		t.Errorf("Check runtime status error: %s", err)
	}

	if err = testLauncher.SendCurrentRuntimeStatus(); err != nil {
		t.Errorf("Can't send current runtime status: %s", err)
	}

	if err = checkRuntimeStatus(runtimeStatus, testLauncher.RuntimeStatusChannel()); err != nil {
		t.Errorf("Check runtime status error: %s", err)
	}
}

func TestStopAllInstances(t *testing.T) {
	runningInstances := make(map[string]runner.InstanceStatus)

	serviceProvider := newTestServiceProvider()
	instanceRunner := newTestRunner(
		func(instanceID string) runner.InstanceStatus {
			status := runner.InstanceStatus{InstanceID: instanceID, State: cloudprotocol.InstanceStateActive}

			runningInstances[instanceID] = status

			return status
		},
		func(instanceID string) error {
			delete(runningInstances, instanceID)

			return nil
		},
	)

	testLauncher, err := launcher.New(&config.Config{WorkingDir: tmpDir}, newTestStorage(), serviceProvider,
		instanceRunner)
	if err != nil {
		t.Fatalf("Can't create launcher: %s", err)
	}
	defer testLauncher.Close()

	testInstances := []testInstance{
		{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
		{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2},
		{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1},
		{serviceID: "service1", serviceVersion: 1, subjectID: "subject2", numInstances: 2},
		{serviceID: "service2", serviceVersion: 2, subjectID: "subject3", numInstances: 2},
		{serviceID: "service2", serviceVersion: 2, subjectID: "subject4", numInstances: 3},
	}

	if err = serviceProvider.fromTestInstances(testInstances); err != nil {
		t.Fatalf("Can't create test services: %s", err)
	}

	if err = testLauncher.RunInstances(createInstancesInfos(testInstances)); err != nil {
		t.Fatalf("Can't run instances: %s", err)
	}

	runtimeStatus := launcher.RuntimeStatus{
		RunStatus: &launcher.RunInstancesStatus{Instances: createInstancesStatuses(testInstances)},
	}

	if err = checkRuntimeStatus(runtimeStatus, testLauncher.RuntimeStatusChannel()); err != nil {
		t.Errorf("Check runtime status error: %s", err)
	}

	if len(runtimeStatus.RunStatus.Instances) != len(runningInstances) {
		t.Errorf("Wrong running instances count: %d", len(runningInstances))
	}

	if err = testLauncher.StopAllInstances(); err != nil {
		t.Errorf("Can't stop instances: %s", err)
	}

	if len(runningInstances) != 0 {
		t.Errorf("Wrong running instances count: %d", len(runningInstances))
	}
}

func TestSubjectsChanged(t *testing.T) {
	type testData struct {
		initialInstances []testInstance
		subjects         []string
		resultInstances  []testInstance
	}

	data := []testData{
		{
			initialInstances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2, unitSubject: true},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1, unitSubject: true},
				{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 4, unitSubject: true},
			},
			subjects: []string{"subject2"},
			resultInstances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 4},
			},
		},
		{
			initialInstances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2, unitSubject: true},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1, unitSubject: true},
				{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 4, unitSubject: true},
			},
			subjects: []string{"subject1"},
			resultInstances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1},
			},
		},
		{
			initialInstances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2, unitSubject: true},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1, unitSubject: true},
				{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 4, unitSubject: true},
			},
			subjects: []string{"subject1", "subject2"},
			resultInstances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1},
				{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 4},
			},
		},
		{
			initialInstances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject1", numInstances: 2, unitSubject: true},
				{serviceID: "service1", serviceVersion: 1, subjectID: "subject1", numInstances: 1, unitSubject: true},
				{serviceID: "service2", serviceVersion: 2, subjectID: "subject2", numInstances: 4, unitSubject: true},
			},
			subjects: []string{"subject3"},
			resultInstances: []testInstance{
				{serviceID: "service0", serviceVersion: 0, subjectID: "subject0", numInstances: 3},
			},
		},
	}

	storage := newTestStorage()
	serviceProvider := newTestServiceProvider()

	testLauncher, err := launcher.New(&config.Config{WorkingDir: tmpDir}, storage, serviceProvider,
		newTestRunner(nil, nil))
	if err != nil {
		t.Fatalf("Can't create launcher: %s", err)
	}
	defer testLauncher.Close()

	for i, item := range data {
		t.Logf("Subjects changed: %d", i)

		if err = serviceProvider.fromTestInstances(item.initialInstances); err != nil {
			t.Fatalf("Can't create test services: %s", err)
		}

		storage.createInstances(item.initialInstances)

		if err = testLauncher.SubjectsChanged(item.subjects); err != nil {
			t.Fatalf("Subjects changed error: %s", err)
		}

		select {
		case runtimeStatus := <-testLauncher.RuntimeStatusChannel():
			runStatus := &launcher.RunInstancesStatus{
				UnitSubjects: item.subjects,
				Instances:    createInstancesStatuses(item.resultInstances),
			}

			if err = compareRuntimeStatus(launcher.RuntimeStatus{RunStatus: runStatus}, runtimeStatus); err != nil {
				t.Errorf("Compare runtime status failed: %s", err)
			}

		case <-time.After(5 * time.Second):
			t.Error("Wait for runtime status timeout")
		}
	}
}

func TestHostFSDir(t *testing.T) {
	hostFSBinds := []string{"bin", "sbin", "lib", "lib64", "usr"}

	testLauncher, err := launcher.New(&config.Config{
		WorkingDir: tmpDir,
		HostBinds:  hostFSBinds,
	},
		newTestStorage(), newTestServiceProvider(), newTestRunner(nil, nil))
	if err != nil {
		t.Fatalf("Can't create launcher: %s", err)
	}
	defer testLauncher.Close()

	rootItems, err := ioutil.ReadDir("/")
	if err != nil {
		t.Fatalf("Can't read root dir: %s", err)
	}

	whiteoutItems, err := ioutil.ReadDir(filepath.Join(tmpDir, "hostfs", "whiteouts"))
	if err != nil {
		t.Fatalf("Can't read root dir: %s", err)
	}

	for _, rootItem := range rootItems {
		bind := false

		for _, hostFSBind := range hostFSBinds {
			if rootItem.Name() == hostFSBind {
				bind = true
				break
			}
		}

		whiteout := false

		for i, whiteoutItem := range whiteoutItems {
			if rootItem.Name() == whiteoutItem.Name() {
				if whiteoutItem.Mode() != 0o410000000 {
					t.Errorf("Wrong white out mode 0o%o", whiteoutItem.Mode())
				}

				whiteoutItems = append(whiteoutItems[:i], whiteoutItems[i+1:]...)
				whiteout = true

				break
			}
		}

		if bind && whiteout {
			t.Errorf("Bind item %s should not be whiteouted", rootItem.Name())
		}

		if !bind && !whiteout {
			t.Errorf("Not bind item %s should be whiteouted", rootItem.Name())
		}
	}
}

/***********************************************************************************************************************
 * testStorage
 **********************************************************************************************************************/

func newTestStorage() *testStorage {
	return &testStorage{
		instances: make(map[string]launcher.InstanceInfo),
	}
}

func (storage *testStorage) AddInstance(instance launcher.InstanceInfo) error {
	storage.Lock()
	defer storage.Unlock()

	if _, ok := storage.instances[instance.InstanceID]; ok {
		return aoserrors.New("instance exists")
	}

	storage.instances[instance.InstanceID] = instance

	return nil
}

func (storage *testStorage) UpdateInstance(instance launcher.InstanceInfo) error {
	storage.Lock()
	defer storage.Unlock()

	if _, ok := storage.instances[instance.InstanceID]; !ok {
		return launcher.ErrNotExist
	}

	storage.instances[instance.InstanceID] = instance

	return nil
}

func (storage *testStorage) RemoveInstance(instanceID string) error {
	storage.Lock()
	defer storage.Unlock()

	if _, ok := storage.instances[instanceID]; !ok {
		return launcher.ErrNotExist
	}

	delete(storage.instances, instanceID)

	return nil
}

func (storage *testStorage) GetInstanceByIndex(
	serviceID, subjectID string, index uint64,
) (launcher.InstanceInfo, error) {
	storage.RLock()
	defer storage.RUnlock()

	for _, instance := range storage.instances {
		if instance.ServiceID == serviceID && instance.SubjectID == subjectID && instance.Index == index {
			return instance, nil
		}
	}

	return launcher.InstanceInfo{}, launcher.ErrNotExist
}

func (storage *testStorage) GetRunningInstances() (instances []launcher.InstanceInfo, err error) {
	storage.RLock()
	defer storage.RUnlock()

	for _, instance := range storage.instances {
		if instance.Running {
			instances = append(instances, instance)
		}
	}

	return instances, nil
}

func (storage *testStorage) GetSubjectInstances(subjectID string) (instances []launcher.InstanceInfo, err error) {
	storage.RLock()
	defer storage.RUnlock()

	for _, instance := range storage.instances {
		if instance.SubjectID == subjectID {
			instances = append(instances, instance)
		}
	}

	return instances, nil
}

func (storage *testStorage) getInstanceByID(instanceID string) (launcher.InstanceInfo, error) {
	storage.RLock()
	defer storage.RUnlock()

	instance, ok := storage.instances[instanceID]
	if !ok {
		return launcher.InstanceInfo{}, launcher.ErrNotExist
	}

	return instance, nil
}

func (storage *testStorage) createInstances(testInstances []testInstance) {
	storage.Lock()
	defer storage.Unlock()

	for _, testInstance := range testInstances {
		for i := uint64(0); i < testInstance.numInstances; i++ {
			newInstanceID := uuid.New().String()

			for instanceID, instance := range storage.instances {
				if instance.ServiceID == testInstance.serviceID && instance.SubjectID == testInstance.subjectID &&
					instance.Index == i {
					newInstanceID = instanceID
				}
			}

			storage.instances[newInstanceID] = launcher.InstanceInfo{
				ServiceID:   testInstance.serviceID,
				SubjectID:   testInstance.subjectID,
				InstanceID:  newInstanceID,
				Index:       i,
				UnitSubject: testInstance.unitSubject,
				Running:     true,
			}
		}
	}
}

/***********************************************************************************************************************
 * testServiceProvider
 **********************************************************************************************************************/

func newTestServiceProvider() *testServiceProvider {
	return &testServiceProvider{
		services: make(map[string]servicemanager.ServiceInfo),
	}
}

func (provider *testServiceProvider) GetServiceInfo(serviceID string) (servicemanager.ServiceInfo, error) {
	service, ok := provider.services[serviceID]
	if !ok {
		return servicemanager.ServiceInfo{}, servicemanager.ErrNotExist
	}

	return service, nil
}

func (provider *testServiceProvider) fromTestInstances(testInstances []testInstance) error {
	provider.services = make(map[string]servicemanager.ServiceInfo)

	for _, testInstance := range testInstances {
		provider.services[testInstance.serviceID] = servicemanager.ServiceInfo{
			ServiceID:  testInstance.serviceID,
			AosVersion: testInstance.serviceVersion,
		}
	}

	return nil
}

/***********************************************************************************************************************
 * testRunner
 **********************************************************************************************************************/

func newTestRunner(startFunc func(instanceID string) runner.InstanceStatus,
	stopFunc func(instanceID string) error,
) *testRunner {
	return &testRunner{
		statusChannel: make(chan []runner.InstanceStatus, 1),
		startFunc:     startFunc,
		stopFunc:      stopFunc,
	}
}

func (instanceRunner *testRunner) StartInstance(
	instanceID, runtimeDir string, params runner.StartInstanceParams,
) runner.InstanceStatus {
	instanceRunner.Lock()
	defer instanceRunner.Unlock()

	if instanceRunner.startFunc == nil {
		return runner.InstanceStatus{
			InstanceID: instanceID,
			State:      cloudprotocol.InstanceStateActive,
		}
	}

	return instanceRunner.startFunc(instanceID)
}

func (instanceRunner *testRunner) StopInstance(instanceID string) error {
	instanceRunner.Lock()
	defer instanceRunner.Unlock()

	if instanceRunner.stopFunc == nil {
		return nil
	}

	return instanceRunner.stopFunc(instanceID)
}

func (instanceRunner *testRunner) InstanceStatusChannel() <-chan []runner.InstanceStatus {
	return instanceRunner.statusChannel
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func setup() (err error) {
	tmpDir, err = ioutil.TempDir("", "sm_")
	if err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func cleanup() (err error) {
	if err = os.RemoveAll(tmpDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func compareRuntimeStatus(status1, status2 launcher.RuntimeStatus) (err error) {
	switch {
	case status1.RunStatus == nil && status2.RunStatus == nil:

	case status1.RunStatus != nil && status2.RunStatus != nil:
		if !compareArrays(len(status1.RunStatus.UnitSubjects), len(status2.RunStatus.UnitSubjects),
			func(index1, index2 int) bool {
				return status1.RunStatus.UnitSubjects[index1] == status2.RunStatus.UnitSubjects[index2]
			}) {
			return aoserrors.New("unit subjects mismatch")
		}

		if !compareArrays(len(status1.RunStatus.Instances), len(status2.RunStatus.Instances),
			func(index1, index2 int) bool {
				return isInstanceStatusesEqual(status1.RunStatus.Instances[index1], status2.RunStatus.Instances[index2])
			}) {
			return aoserrors.New("run instances statuses mismatch")
		}

		if !compareArrays(len(status1.RunStatus.ErrorServices), len(status2.RunStatus.ErrorServices),
			func(index1, index2 int) bool {
				return status1.RunStatus.ErrorServices[index1] == status2.RunStatus.ErrorServices[index2]
			}) {
			return aoserrors.New("error services mismatch")
		}

	case status1.RunStatus == nil || status2.RunStatus == nil:
		return aoserrors.New("run status mismatch")
	}

	switch {
	case status1.UpdateStatus == nil && status2.UpdateStatus == nil:

	case status1.UpdateStatus != nil && status2.UpdateStatus != nil:
		if !compareArrays(len(status1.UpdateStatus.Instances), len(status2.UpdateStatus.Instances),
			func(index1, index2 int) bool {
				return isInstanceStatusesEqual(status1.UpdateStatus.Instances[index1],
					status2.UpdateStatus.Instances[index2])
			}) {
			return aoserrors.New("update instances statuses mismatch")
		}

	case status1.UpdateStatus == nil || status2.UpdateStatus == nil:
		return aoserrors.New("update status mismatch")
	}

	return nil
}

func isInstanceStatusesEqual(status1, status2 cloudprotocol.InstanceStatus) bool {
	switch {
	case status1.ErrorInfo == nil && status2.ErrorInfo == nil:

	case status1.ErrorInfo != nil && status2.ErrorInfo != nil:
		if status1.ErrorInfo.AosCode != status2.ErrorInfo.AosCode ||
			status1.ErrorInfo.ExitCode != status2.ErrorInfo.ExitCode ||
			!(strings.HasPrefix(status1.ErrorInfo.Message, status2.ErrorInfo.Message) ||
				(strings.HasPrefix(status2.ErrorInfo.Message, status1.ErrorInfo.Message))) {
			return false
		}

	case status1.ErrorInfo == nil || status2.ErrorInfo == nil:
		return false
	}

	status1.ErrorInfo, status2.ErrorInfo = nil, nil

	return status1 == status2
}

func compareArrays(len1, len2 int, isEqual func(index1, index2 int) bool) bool {
	if len1 != len2 {
		return false
	}

loop1:
	for i := 0; i < len1; i++ {
		for j := 0; j < len2; j++ {
			if isEqual(i, j) {
				continue loop1
			}
		}

		return false
	}

loop2:

	for j := 0; j < len2; j++ {
		for i := 0; i < len1; i++ {
			if isEqual(i, j) {
				continue loop2
			}
		}

		return false
	}

	return true
}

func checkRuntimeStatus(refStatus launcher.RuntimeStatus, statusChannel <-chan launcher.RuntimeStatus) error {
	select {
	case runtimeStatus := <-statusChannel:
		if err := compareRuntimeStatus(refStatus, runtimeStatus); err != nil {
			return aoserrors.Wrap(err)
		}

	case <-time.After(5 * time.Second):
		return aoserrors.New("Wait for runtime status timeout")
	}

	return nil
}

func createInstancesInfos(testInstances []testInstance) (instances []cloudprotocol.InstanceInfo) {
	for _, testInstance := range testInstances {
		instances = append(instances, cloudprotocol.InstanceInfo{
			ServiceID:    testInstance.serviceID,
			SubjectID:    testInstance.subjectID,
			NumInstances: testInstance.numInstances,
		})
	}

	return instances
}

func createInstancesStatuses(testInstances []testInstance) (instances []cloudprotocol.InstanceStatus) {
	for _, testInstance := range testInstances {
		for index := uint64(0); index < testInstance.numInstances; index++ {
			instanceStatus := cloudprotocol.InstanceStatus{
				ServiceID:  testInstance.serviceID,
				AosVersion: testInstance.serviceVersion,
				SubjectID:  testInstance.subjectID,
				Instance:   index,
				RunState:   cloudprotocol.InstanceStateActive,
			}

			if testInstance.err != nil {
				instanceStatus.RunState = cloudprotocol.InstanceStateFailed
				instanceStatus.ErrorInfo = &cloudprotocol.ErrorInfo{
					Message: testInstance.err.Error(),
				}
			}

			instances = append(instances, instanceStatus)
		}
	}

	return instances
}

func getRunnerStatus(instanceID string, testInstances []testInstance, storage *testStorage) runner.InstanceStatus {
	activeStatus := runner.InstanceStatus{InstanceID: instanceID, State: cloudprotocol.InstanceStateActive}

	instance, err := storage.getInstanceByID(instanceID)
	if err != nil {
		return activeStatus
	}

	for _, testInstance := range testInstances {
		if testInstance.serviceID == instance.ServiceID && testInstance.subjectID == instance.SubjectID {
			if testInstance.err != nil {
				return runner.InstanceStatus{
					InstanceID: instanceID,
					State:      cloudprotocol.InstanceStateFailed,
					Err:        testInstance.err,
				}
			}

			return activeStatus
		}
	}

	return activeStatus
}

func createRunStatus(storage *testStorage,
	testInstances []testInstance,
) (runStatus []runner.InstanceStatus, err error) {
	for _, testInstance := range testInstances {
		for i := uint64(0); i < testInstance.numInstances; i++ {
			instance, err := storage.GetInstanceByIndex(
				testInstance.serviceID, testInstance.subjectID, i)
			if err != nil {
				return nil, aoserrors.Wrap(err)
			}

			state := cloudprotocol.InstanceStateActive

			if testInstance.err != nil {
				state = cloudprotocol.InstanceStateFailed
			}

			runStatus = append(runStatus, runner.InstanceStatus{
				InstanceID: instance.InstanceID,
				State:      state,
				Err:        testInstance.err,
			})
		}
	}

	return runStatus, nil
}

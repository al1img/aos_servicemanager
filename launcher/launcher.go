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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_common/utils/action"
	"github.com/aoscloud/aos_common/utils/fs"
	"github.com/google/uuid"
	runtimespec "github.com/opencontainers/runtime-spec/specs-go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	"github.com/aoscloud/aos_servicemanager/config"
	"github.com/aoscloud/aos_servicemanager/layermanager"
	"github.com/aoscloud/aos_servicemanager/monitoring"
	"github.com/aoscloud/aos_servicemanager/networkmanager"
	"github.com/aoscloud/aos_servicemanager/resourcemanager"
	"github.com/aoscloud/aos_servicemanager/runner"
	"github.com/aoscloud/aos_servicemanager/servicemanager"
	"github.com/aoscloud/aos_servicemanager/storagestate"
	"github.com/aoscloud/aos_servicemanager/utils/uidgidpool"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const maxParallelInstanceActions = 32

const (
	runtimeDir             = "/run/aos/runtime"
	hostFSWiteoutsDir      = "hostfs/whiteouts"
	runtimeConfigFile      = "config.json"
	instanceRootFS         = "rootfs"
	instanceMountPointsDir = "mounts"
	instanceStateFile      = "/state.dat"
	upperDirName           = "upperdir"
	workDirName            = "workdir"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Storage storage interface.
type Storage interface {
	AddInstance(instance InstanceInfo) error
	UpdateInstance(instance InstanceInfo) error
	RemoveInstance(instanceID string) error
	GetInstanceByIndex(serviceID, subjectID string, index uint64) (InstanceInfo, error)
	GetAllInstances() ([]InstanceInfo, error)
	GetRunningInstances() ([]InstanceInfo, error)
	GetSubjectInstances(subjectID string) ([]InstanceInfo, error)
}

// ServiceProvider service provider.
type ServiceProvider interface {
	GetServiceInfo(serviceID string) (servicemanager.ServiceInfo, error)
	GetImageParts(service servicemanager.ServiceInfo) (servicemanager.ImageParts, error)
}

// LayerProvider layer provider.
type LayerProvider interface {
	GetLayerInfoByDigest(digest string) (layermanager.LayerInfo, error)
}

// InstanceRunner interface to start/stop service instances.
type InstanceRunner interface {
	StartInstance(instanceID, runtimeDir string, params runner.StartInstanceParams) runner.InstanceStatus
	StopInstance(instanceID string) error
	InstanceStatusChannel() <-chan []runner.InstanceStatus
}

// ResourceManager provides API to validate, request and release resources.
type ResourceManager interface {
	GetDeviceInfo(name string) (resourcemanager.DeviceInfo, error)
	GetResourceInfo(name string) (resourcemanager.ResourceInfo, error)
	AllocateDevice(instanceID string, name string) error
	ReleaseDevices(instanceID string) error
}

// NetworkManager provides network access.
type NetworkManager interface {
	GetNetnsPath(instanceID string) string
	AddInstanceToNetwork(instanceID, networkID string, params networkmanager.NetworkParams) error
	RemoveInstanceFromNetwork(instanceID, networkID string) error
	GetInstanceIP(instanceID, networkID string) (string, error)
}

// InstanceRegistrar provides API to register/unregister instance.
type InstanceRegistrar interface {
	RegisterInstance(instanceID string, permissions map[string]map[string]string) (secret string, err error)
	UnregisterInstance(instanceID string) error
}

// StorageProvider provides API for instance storage.
type StorageProvider interface {
	PrepareStorage(instanceID string, uid, gid int, quota uint64) (path string, err error)
	ReleaseStorage(instanceID string) error
}

// StateProvider provides API for instance state.
type StateProvider interface {
	PrepareState(instanceID string, uid, gid int, quota uint64) (path string, checksum []byte, err error)
	ReleaseState(instanceID string) error
	StateChangedChannel() <-chan storagestate.StateChangedInfo
}

// InstanceMonitor provides API to monitor instance parameters.
type InstanceMonitor interface {
	StartInstanceMonitor(instanceID string, params monitoring.MonitorParams) error
	StopInstanceMonitor(instanceID string) error
}

// InstanceInfo instance information.
type InstanceInfo struct {
	ServiceID   string
	SubjectID   string
	InstanceID  string
	Index       uint64
	UnitSubject bool
	Running     bool
	UID         int
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

	storage           Storage
	serviceProvider   ServiceProvider
	layerProvider     LayerProvider
	instanceRunner    InstanceRunner
	resourceManager   ResourceManager
	networkManager    NetworkManager
	instanceRegistrar InstanceRegistrar
	storageProvider   StorageProvider
	stateProvider     StateProvider
	instanceMonitor   InstanceMonitor

	config                 *config.Config
	currentSubjects        []string
	runtimeStatusChannel   chan RuntimeStatus
	cancelFunction         context.CancelFunc
	actionHandler          *action.Handler
	runMutex               sync.RWMutex
	runInstancesInProgress bool
	currentInstances       map[string]*instanceInfo
	currentServices        map[string]*serviceInfo
	uidPool                *uidgidpool.IdentifierPool
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

// Mount, unmount instance FS functions.
// nolint:gochecknoglobals
var (
	MountFunc   = fs.OverlayMount
	UnmountFunc = fs.Umount
)

var (
	// ErrNotExist not exist instance error.
	ErrNotExist = errors.New("instance not exist")
	// ErrNoRuntimeStatus no current runtime status error.
	ErrNoRuntimeStatus = errors.New("no runtime status")
)

var defaultHostFSBinds = []string{"bin", "sbin", "lib", "lib64", "usr"} // nolint:gochecknoglobals // const

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new launcher object.
func New(config *config.Config, storage Storage, serviceProvider ServiceProvider, layerProvider LayerProvider,
	instanceRunner InstanceRunner, resourceManager ResourceManager, networkManager NetworkManager,
	instanceRegistrar InstanceRegistrar, storageProvider StorageProvider, stateProvider StateProvider,
	instanceMonitor InstanceMonitor,
) (launcher *Launcher, err error) {
	log.WithField("runner", config.Runner).Debug("New launcher")

	launcher = &Launcher{
		storage: storage, serviceProvider: serviceProvider, layerProvider: layerProvider,
		instanceRunner: instanceRunner, resourceManager: resourceManager, networkManager: networkManager,
		instanceRegistrar: instanceRegistrar, storageProvider: storageProvider, stateProvider: stateProvider,
		instanceMonitor: instanceMonitor,

		config:               config,
		actionHandler:        action.New(maxParallelInstanceActions),
		runtimeStatusChannel: make(chan RuntimeStatus, 1),
		uidPool:              uidgidpool.NewUserIDPool(),
	}

	launcher.fillUIDPool()

	ctx, cancelFunction := context.WithCancel(context.Background())

	launcher.cancelFunction = cancelFunction

	go launcher.handleRunnerStatus(ctx)
	go launcher.handleStateChanged(ctx)

	if err = launcher.prepareHostFSDir(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err = os.MkdirAll(runtimeDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

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

	if removeErr := os.RemoveAll(runtimeDir); removeErr != nil {
		if err == nil {
			err = removeErr
		}
	}

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

func (launcher *Launcher) fillUIDPool() {
	instances, err := launcher.storage.GetAllInstances()
	if err != nil {
		log.Errorf("Can't fill UID pool: %s", err)
	}

	for _, instance := range instances {
		if err = launcher.uidPool.AddID(instance.UID); err != nil {
			log.WithFields(instance.logFields()).Errorf("Can't add UID to pool: %s", err)
		}
	}
}

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

func (launcher *Launcher) handleStateChanged(ctx context.Context) {
	for {
		select {
		case stateChangedInfo := <-launcher.stateProvider.StateChangedChannel():
			launcher.updateInstanceState(stateChangedInfo)

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

func (launcher *Launcher) updateInstanceState(stateChangedIngo storagestate.StateChangedInfo) {
	launcher.runMutex.Lock()
	defer launcher.runMutex.Unlock()

	instance, ok := launcher.currentInstances[stateChangedIngo.InstanceID]
	if !ok {
		log.WithField("instanceID", stateChangedIngo.InstanceID).Errorf("Unknown instance state changed")

		return
	}

	if !bytes.Equal(stateChangedIngo.Checksum, instance.stateChecksum) {
		log.WithFields(log.Fields{
			"instanceID": stateChangedIngo.InstanceID,
			"checksum":   hex.EncodeToString(stateChangedIngo.Checksum),
		}).Debugf("Instance state changed")

		instance.stateChecksum = stateChangedIngo.Checksum
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

	uid, err := launcher.uidPool.GetFreeID()
	if err != nil {
		return instance, aoserrors.Wrap(err)
	}

	instance.UID = uid

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

func (launcher *Launcher) releaseRuntime(instance *instanceInfo, service *serviceInfo) (err error) {
	if service == nil || service.serviceConfig.Permissions != nil {
		if registerErr := launcher.instanceRegistrar.UnregisterInstance(instance.InstanceID); registerErr != nil {
			if err == nil {
				err = aoserrors.Wrap(registerErr)
			}
		}
	}

	if storageErr := launcher.storageProvider.ReleaseStorage(instance.InstanceID); storageErr != nil {
		if err == nil {
			err = aoserrors.Wrap(storageErr)
		}
	}

	if stateErr := launcher.stateProvider.ReleaseState(instance.InstanceID); stateErr != nil {
		if err == nil {
			err = aoserrors.Wrap(stateErr)
		}
	}

	if networkErr := launcher.networkManager.RemoveInstanceFromNetwork(
		instance.InstanceID, service.ServiceProvider); networkErr != nil {
		if err == nil {
			err = aoserrors.Wrap(networkErr)
		}
	}

	if deviceErr := launcher.resourceManager.ReleaseDevices(instance.InstanceID); deviceErr != nil {
		if err == nil {
			err = aoserrors.Wrap(deviceErr)
		}
	}

	return err
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
			err = aoserrors.Wrap(runnerErr)
		}
	}

	service, serviceErr := launcher.getCurrentServiceInfo(instance.ServiceID)
	if serviceErr != nil {
		if err == nil {
			err = aoserrors.Wrap(serviceErr)
		}
	}

	if releaseErr := launcher.releaseRuntime(instance, service); releaseErr != nil {
		if err == nil {
			err = aoserrors.Wrap(releaseErr)
		}
	}

	if unmountErr := UnmountFunc(filepath.Join(instance.runtimeDir, instanceRootFS)); unmountErr != nil {
		if err == nil {
			err = aoserrors.Wrap(unmountErr)
		}
	}

	if removeErr := os.RemoveAll(filepath.Join(runtimeDir, instance.InstanceID)); removeErr != nil {
		if err == nil {
			err = removeErr
		}
	}

	if monitorErr := launcher.instanceMonitor.StopInstanceMonitor(instance.InstanceID); monitorErr != nil {
		log.WithFields(instance.logFields()).Errorf("Can't stop instance monitoring: %s", monitorErr)
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

func (launcher *Launcher) getHostsFromResources(resources []string) (hosts []config.Host, err error) {
	for _, resource := range resources {
		boardResource, err := launcher.resourceManager.GetResourceInfo(resource)
		if err != nil {
			return hosts, aoserrors.Wrap(err)
		}

		hosts = append(hosts, boardResource.Hosts...)
	}

	return hosts, nil
}

func (launcher *Launcher) setupNetwork(instance *instanceInfo, service *serviceInfo) (err error) {
	networkFilesDir := filepath.Join(instance.runtimeDir, instanceMountPointsDir)

	if err = os.MkdirAll(networkFilesDir, 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	params := networkmanager.NetworkParams{
		HostsFilePath:      filepath.Join(networkFilesDir, "etc", "hosts"),
		ResolvConfFilePath: filepath.Join(networkFilesDir, "etc", "resolv.conf"),
	}

	if service.serviceConfig.Quotas.DownloadSpeed != nil {
		params.IngressKbit = *service.serviceConfig.Quotas.DownloadSpeed
	}

	if service.serviceConfig.Quotas.UploadSpeed != nil {
		params.EgressKbit = *service.serviceConfig.Quotas.UploadSpeed
	}

	if service.serviceConfig.Quotas.DownloadLimit != nil {
		params.DownloadLimit = *service.serviceConfig.Quotas.DownloadLimit
	}

	if service.serviceConfig.Quotas.UploadLimit != nil {
		params.UploadLimit = *service.serviceConfig.Quotas.UploadLimit
	}

	if service.serviceConfig.Hostname != nil {
		params.Hostname = *service.serviceConfig.Hostname
	}

	params.ExposedPorts = make([]string, 0, len(service.imageConfig.Config.ExposedPorts))

	for key := range service.imageConfig.Config.ExposedPorts {
		params.ExposedPorts = append(params.ExposedPorts, key)
	}

	params.AllowedConnections = make([]string, 0, len(service.serviceConfig.AllowedConnections))

	for key := range service.serviceConfig.AllowedConnections {
		params.AllowedConnections = append(params.AllowedConnections, key)
	}

	if params.Hosts, err = launcher.getHostsFromResources(service.serviceConfig.Resources); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := launcher.networkManager.AddInstanceToNetwork(
		instance.InstanceID, service.ServiceProvider, params); err != nil {
		return aoserrors.Wrap(err)
	}

	if instance.ipAddress, err = launcher.networkManager.GetInstanceIP(
		instance.InstanceID, service.ServiceProvider); err != nil {
		log.WithFields(instance.logFields()).Errorf("Can't get instance IP: %s", err)
	}

	return nil
}

func (launcher *Launcher) allocateDevices(instance *instanceInfo, devices []serviceDevice) (err error) {
	defer func() {
		if err != nil {
			if releaseErr := launcher.resourceManager.ReleaseDevices(instance.InstanceID); releaseErr != nil {
				log.WithFields(instance.logFields()).Errorf("Can't release instance devices: %s", releaseErr)
			}
		}
	}()

	for _, device := range devices {
		if err := launcher.resourceManager.AllocateDevice(instance.InstanceID, device.Name); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	// TODO: Resource alert

	return nil
}

func (launcher *Launcher) setupRuntime(instance *instanceInfo, service *serviceInfo) error {
	if service.serviceConfig.Permissions != nil {
		secret, err := launcher.instanceRegistrar.RegisterInstance(
			instance.InstanceID, service.serviceConfig.Permissions)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		instance.secret = secret
	}

	storageLimit := uint64(0)

	if service.serviceConfig.Quotas.StorageLimit != nil {
		storageLimit = *service.serviceConfig.Quotas.StorageLimit
	}

	storagePath, err := launcher.storageProvider.PrepareStorage(
		instance.InstanceID, instance.UID, service.GID, storageLimit)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	instance.storagePath = storagePath

	stateLimit := uint64(0)

	if service.serviceConfig.Quotas.StateLimit != nil {
		stateLimit = *service.serviceConfig.Quotas.StateLimit
	}

	statePath, stateChecksum, err := launcher.stateProvider.PrepareState(
		instance.InstanceID, instance.UID, service.GID, stateLimit)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	instance.statePath = statePath

	// State checksum can be changed in state changed handler
	launcher.runMutex.Lock()
	instance.stateChecksum = stateChecksum
	launcher.runMutex.Unlock()

	if err = launcher.setupNetwork(instance, service); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = launcher.allocateDevices(instance, service.serviceConfig.Devices); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
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

	if err := os.MkdirAll(instance.runtimeDir, 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := launcher.setupRuntime(instance, service); err != nil {
		return aoserrors.Wrap(err)
	}

	runtimeSpec, err := launcher.createRuntimeSpec(instance, service)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := launcher.prepareRootFS(instance, service, runtimeSpec); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := launcher.instanceMonitor.StartInstanceMonitor(
		instance.InstanceID, monitoring.MonitorParams{
			UID:         instance.UID,
			GID:         service.GID,
			IPAddress:   instance.ipAddress,
			StoragePath: instance.storagePath,
			AlertRules:  service.serviceConfig.AlertRules,
		}); err != nil {
		log.WithFields(instance.logFields()).Errorf("Can't start instance monitoring: %s", err)
	}

	runStatus := launcher.instanceRunner.StartInstance(
		instance.InstanceID, instance.runtimeDir, runner.StartInstanceParams{})

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

func (launcher *Launcher) prepareHostFSDir() (err error) {
	witeoutsDir := path.Join(launcher.config.WorkingDir, hostFSWiteoutsDir)

	if err = os.MkdirAll(witeoutsDir, 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	allowedDirs := defaultHostFSBinds

	if len(launcher.config.HostBinds) > 0 {
		allowedDirs = launcher.config.HostBinds
	}

	rootContent, err := ioutil.ReadDir("/")
	if err != nil {
		return aoserrors.Wrap(err)
	}

rootLabel:
	for _, item := range rootContent {
		itemPath := path.Join(witeoutsDir, item.Name())

		if _, err = os.Stat(itemPath); err == nil {
			// skip already exists items
			continue
		}

		if !os.IsNotExist(err) {
			return aoserrors.Wrap(err)
		}

		for _, allowedItem := range allowedDirs {
			if item.Name() == allowedItem {
				continue rootLabel
			}
		}

		// Create whiteout for not allowed items
		if err = syscall.Mknod(itemPath, syscall.S_IFCHR, int(unix.Mkdev(0, 0))); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func prepareStorageDir(path string, uid, gid int) (upperDir, workDir string, err error) {
	upperDir = filepath.Join(path, upperDirName)
	workDir = filepath.Join(path, workDirName)

	if err = os.MkdirAll(upperDir, 0o755); err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	if err = os.Chown(upperDir, uid, gid); err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	if err = os.MkdirAll(workDir, 0o755); err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	if err = os.Chown(workDir, uid, gid); err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	return upperDir, workDir, nil
}

func (launcher *Launcher) prepareRootFS(
	instance *instanceInfo, service *serviceInfo, runtimeConfig *runtimeSpec,
) error {
	mountPointsDir := filepath.Join(instance.runtimeDir, instanceMountPointsDir)

	if err := launcher.createMountPoints(mountPointsDir, runtimeConfig.ociSpec.Mounts); err != nil {
		return aoserrors.Wrap(err)
	}

	imageParts, err := launcher.serviceProvider.GetImageParts(service.ServiceInfo)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	layersDir := []string{mountPointsDir, imageParts.ServiceFSPath}

	for _, digest := range imageParts.LayersDigest {
		layer, err := launcher.layerProvider.GetLayerInfoByDigest(digest)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		layersDir = append(layersDir, layer.Path)
	}

	layersDir = append(layersDir, path.Join(launcher.config.WorkingDir, hostFSWiteoutsDir), "/")

	rootfsDir := filepath.Join(instance.runtimeDir, instanceRootFS)

	if err = os.MkdirAll(rootfsDir, 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	var upperDir, workDir string

	if instance.storagePath != "" {
		if upperDir, workDir, err = prepareStorageDir(instance.storagePath, instance.UID, service.GID); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	if err = MountFunc(rootfsDir, layersDir, workDir, upperDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func getMountPermissions(mount runtimespec.Mount) (permissions uint64, err error) {
	for _, option := range mount.Options {
		nameValue := strings.Split(strings.TrimSpace(option), "=")

		if len(nameValue) > 1 && nameValue[0] == "mode" {
			if permissions, err = strconv.ParseUint(nameValue[1], 8, 32); err != nil {
				return 0, aoserrors.Wrap(err)
			}
		}
	}

	return permissions, nil
}

func createMountPoint(path string, mount runtimespec.Mount, isDir bool) error {
	permissions, err := getMountPermissions(mount)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	mountPoint := filepath.Join(path, mount.Destination)

	if isDir {
		if err := os.MkdirAll(mountPoint, 0o755); err != nil {
			return aoserrors.Wrap(err)
		}
	} else {
		if err = os.MkdirAll(filepath.Dir(mountPoint), 0o755); err != nil {
			return aoserrors.Wrap(err)
		}

		file, err := os.OpenFile(mountPoint, os.O_CREATE, 0o644)
		if err != nil {
			return aoserrors.Wrap(err)
		}
		defer file.Close()
	}

	if permissions != 0 {
		if err := os.Chmod(mountPoint, os.FileMode(permissions)); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (launcher *Launcher) createMountPoints(path string, mounts []runtimespec.Mount) error {
	for _, mount := range mounts {
		switch mount.Type {
		case "proc", "tmpfs", "sysfs":
			if err := createMountPoint(path, mount, true); err != nil {
				return aoserrors.Wrap(err)
			}

		case "bind":
			stat, err := os.Stat(mount.Source)
			if err != nil {
				return aoserrors.Wrap(err)
			}

			if err := createMountPoint(path, mount, stat.IsDir()); err != nil {
				return aoserrors.Wrap(err)
			}
		}
	}

	return nil
}

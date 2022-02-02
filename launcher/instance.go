// SPX-License-Identifier: Apache-2.0
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

package launcher

import (
	"encoding/hex"
	"path/filepath"

	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_servicemanager/runner"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type instanceInfo struct {
	InstanceInfo
	runStatus      runner.InstanceStatus
	runtimeDir     string
	serviceVersion uint64
	stateChecksum  []byte
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *instanceInfo) cloudStatus() cloudprotocol.InstanceStatus {
	status := cloudprotocol.InstanceStatus{
		ServiceID:     instance.ServiceID,
		AosVersion:    instance.serviceVersion,
		SubjectID:     instance.SubjectID,
		Instance:      instance.Index,
		StateChecksum: hex.EncodeToString(instance.stateChecksum),
		RunState:      instance.runStatus.State,
	}

	if status.RunState == cloudprotocol.InstanceStateFailed {
		status.ErrorInfo = &cloudprotocol.ErrorInfo{ExitCode: instance.runStatus.ExitCode}

		if instance.runStatus.Err != nil {
			status.ErrorInfo.Message = instance.runStatus.Err.Error()
		}
	}

	return status
}

func (launcher *Launcher) addInstanceToCurrentList(instance InstanceInfo) *instanceInfo {
	launcher.runMutex.Lock()
	defer launcher.runMutex.Unlock()

	info := &instanceInfo{
		InstanceInfo: instance,
		runtimeDir:   filepath.Join(runtimeDir, instance.InstanceID),
	}

	launcher.currentInstances[instance.InstanceID] = info

	return info
}

func (launcher *Launcher) removeInstanceFromCurrentList(instanceID string) {
	launcher.runMutex.Lock()
	defer launcher.runMutex.Unlock()

	delete(launcher.currentInstances, instanceID)
}

func (launcher *Launcher) getCurrentInstanceByIndex(serviceID, subjectID string, index uint64) (InstanceInfo, error) {
	for _, currentInstance := range launcher.currentInstances {
		if currentInstance.ServiceID == serviceID &&
			currentInstance.SubjectID == subjectID &&
			currentInstance.Index == index {
			return currentInstance.InstanceInfo, nil
		}
	}

	return InstanceInfo{}, ErrNotExist
}

func (instance *InstanceInfo) logFields() log.Fields {
	return log.Fields{
		"serviceID": instance.ServiceID,
		"subjectID": instance.SubjectID,
		"instance":  instance.Index,
	}
}

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
	"reflect"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (launcher *Launcher) setEnvVars(
	envVarsInfo []cloudprotocol.EnvVarsInstanceInfo,
) (envVarsStatus []cloudprotocol.EnvVarsInstanceStatus) {
	envVarsStatus = make([]cloudprotocol.EnvVarsInstanceStatus, 0, len(envVarsInfo))

	now := time.Now()

	// TODO: consider to simplify overriding env var status

	for _, envVarInfo := range envVarsInfo {
		envVarStatus := cloudprotocol.EnvVarsInstanceStatus{
			ServiceID: envVarInfo.ServiceID,
			SubjectID: envVarInfo.SubjectID,
			Instance:  envVarInfo.Instance,
		}

		for _, envVar := range envVarInfo.EnvVars {
			if envVar.TTL.Before(now) {
				err := aoserrors.New("environment variable expired")

				envVarStatus.Statuses = append(envVarStatus.Statuses, cloudprotocol.EnvVarStatus{
					ID: envVar.ID, Error: err.Error(),
				})

				log.WithField("id", envVar.ID).Errorf("Error overriding environment variable: %s", err)

				continue
			}

			envVarStatus.Statuses = append(envVarStatus.Statuses, cloudprotocol.EnvVarStatus{ID: envVar.ID})
		}

		envVarsStatus = append(envVarsStatus, envVarStatus)
	}

	launcher.currentEnvVars = envVarsInfo

	if err := launcher.storage.SetOverrideEnvVars(envVarsInfo); err != nil {
		return setEnvVarsErr(envVarsStatus, err)
	}

	return envVarsStatus
}

func setEnvVarsErr(
	envVarsStatus []cloudprotocol.EnvVarsInstanceStatus, err error,
) []cloudprotocol.EnvVarsInstanceStatus {
	var errStr string

	if err != nil {
		errStr = err.Error()
	}

	for _, envVarStatus := range envVarsStatus {
		for i, status := range envVarStatus.Statuses {
			if status.Error == "" {
				envVarStatus.Statuses[i] = cloudprotocol.EnvVarStatus{
					ID: status.ID, Error: errStr,
				}
			}
		}
	}

	return envVarsStatus
}

func (launcher *Launcher) getInstanceEnvVars(instance InstanceInfo) (envVars []string) {
	now := time.Now()

	for _, envVarInfo := range launcher.currentEnvVars {
		if (envVarInfo.ServiceID == instance.ServiceID) &&
			(envVarInfo.SubjectID == nil || envVarInfo.SubjectID == &instance.SubjectID) &&
			(envVarInfo.Instance == nil || envVarInfo.Instance == &instance.Index) {
			for _, envVar := range envVarInfo.EnvVars {
				if envVar.TTL.Before(now) {
					log.WithFields(instance.logFields()).Debugf(
						"Skip expired environment variable: %s", envVar.Variable)

					continue
				}

				envVars = append(envVars, envVar.Variable)
			}
		}
	}

	return envVars
}

func (launcher *Launcher) updateInstancesEnvVars() {
	launcher.runMutex.RLock()

instancesLoop:
	for _, instance := range launcher.currentInstances {
		if !reflect.DeepEqual(launcher.getInstanceEnvVars(instance.InstanceInfo), instance.envVars) {
			log.WithFields(instance.logFields()).Info("Restart instance due to environment variables change")

			launcher.doStartAction(instance.InstanceInfo, true)

			continue instancesLoop
		}
	}

	launcher.runMutex.RUnlock()

	launcher.actionHandler.Wait()
}

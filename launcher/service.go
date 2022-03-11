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
	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_servicemanager/servicemanager"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type serviceInfo struct {
	servicemanager.ServiceInfo
	serviceConfig *serviceConfig
	imageConfig   *imagespec.Image
	err           error
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (launcher *Launcher) getCurrentServices(instances []InstanceInfo) (currentServices map[string]*serviceInfo) {
	launcher.runMutex.Lock()
	defer launcher.runMutex.Unlock()

	currentServices = make(map[string]*serviceInfo)

	for _, instance := range instances {
		if _, ok := currentServices[instance.ServiceID]; ok {
			continue
		}

		var service serviceInfo

		if service.ServiceInfo, service.err = launcher.serviceProvider.GetServiceInfo(
			instance.ServiceID); service.err != nil {
			log.WithField("serviceID", instance.ServiceID).Errorf("Can't get service info: %s", service.err)
		}

		if service.err == nil {
			if service.serviceConfig, service.err = launcher.getServiceConfig(service.ServiceInfo); service.err != nil {
				log.WithField("serviceID", instance.ServiceID).Errorf("Can't get service config: %s", service.err)
			}
		}

		if service.err == nil {
			if service.imageConfig, service.err = launcher.getImageConfig(service.ServiceInfo); service.err != nil {
				log.WithField("serviceID", instance.ServiceID).Errorf("Can't get image config: %s", service.err)
			}
		}

		if service.err == nil {
			if service.err = launcher.serviceProvider.ValidateService(service.ServiceInfo); service.err != nil {
				log.WithField("serviceID", instance.ServiceID).Errorf("Validate service error: %s", service.err)
			}
		}

		currentServices[instance.ServiceID] = &service
	}

	return currentServices
}

func (launcher *Launcher) getCurrentServiceInfo(serviceID string) (*serviceInfo, error) {
	launcher.runMutex.RLock()
	defer launcher.runMutex.RUnlock()

	service, ok := launcher.currentServices[serviceID]
	if !ok {
		return nil, aoserrors.Errorf("service info is not available: %s", serviceID)
	}

	return service, service.err
}

func (service *serviceInfo) cloudStatus(status string, err error) cloudprotocol.ServiceStatus {
	serviceStatus := cloudprotocol.ServiceStatus{
		ID:         service.ServiceID,
		AosVersion: service.AosVersion,
		Status:     status,
	}

	if err != nil {
		serviceStatus.ErrorInfo = &cloudprotocol.ErrorInfo{Message: err.Error()}
	}

	return serviceStatus
}

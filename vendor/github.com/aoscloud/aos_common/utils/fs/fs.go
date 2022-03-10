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

package fs

import (
	"context"
	"os"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/utils/retryhelper"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	retryCount = 3
	retryDelay = 1 * time.Second
)

const folderPerm = 0o755

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// Mount creates mount point and mount source to it.
func Mount(source string, mountPoint string, fsType string, flags uintptr, opts string) error {
	log.WithFields(log.Fields{"source": source, "type": fsType, "mountPoint": mountPoint}).Debug("Mount dir")

	if err := os.MkdirAll(mountPoint, folderPerm); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := retryhelper.Retry(context.Background(), func() error {
		return aoserrors.Wrap(syscall.Mount(source, mountPoint, fsType, flags, opts))
	}, func(retryCount int, delay time.Duration, err error) {
		log.Warningf("Mount error: %s, try remount...", err)

		forceUmount(mountPoint)
	}, retryCount, retryDelay, 0); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// OverlayMount creates mount point and mount overlay FS to it.
func OverlayMount(mountPoint string, lowerDirs []string, workDir, upperDir string) error {
	opts := "lowerdir=" + strings.Join(lowerDirs, ":")

	if upperDir != "" {
		if workDir == "" {
			return aoserrors.New("working dir path should be set")
		}

		if err := os.RemoveAll(workDir); err != nil {
			return aoserrors.Wrap(err)
		}

		if err := os.MkdirAll(workDir, 0o755); err != nil {
			return aoserrors.Wrap(err)
		}

		opts = opts + ",workdir=" + workDir + ",upperdir=" + upperDir
	}

	if err := Mount("overlay", mountPoint, "overlay", 0, opts); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// Umount umount mount point and remove it.
func Umount(mountPoint string) (err error) {
	log.WithFields(log.Fields{"mountPoint": mountPoint}).Debug("Umount dir")

	defer func() {
		if removeErr := os.RemoveAll(mountPoint); removeErr != nil {
			log.Errorf("Can't remove mount point: %s", removeErr)

			if err == nil {
				err = aoserrors.Wrap(removeErr)
			}
		}
	}()

	if err = retryhelper.Retry(context.Background(), func() error {
		syscall.Sync()

		return aoserrors.Wrap(syscall.Unmount(mountPoint, 0))
	}, func(retryCount int, delay time.Duration, err error) {
		log.Warningf("Unmount error: %s, retry...", err)

		forceUmount(mountPoint)
	}, retryCount, retryDelay, 0); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func forceUmount(mountPoint string) {
	syscall.Sync()
	_ = syscall.Unmount(mountPoint, syscall.MNT_FORCE)
}

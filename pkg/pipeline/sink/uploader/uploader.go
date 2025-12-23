// Copyright 2023 LiveKit, Inc.
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

package uploader

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/errors"
	"github.com/livekit/egress/pkg/stats"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/psrpc"
	"github.com/livekit/storage"
)

const presignedExpiration = time.Hour * 24 * 7 // 7 days

type Uploader struct {
	primary       *store
	backup        *store
	PrimaryGlobal *s3.Client
	BackupGlobal  *s3.Client
	primaryFailed bool
	info          *livekit.EgressInfo
	monitor       *stats.HandlerMonitor
}

type store struct {
	storage.Storage
	conf *config.StorageConfig
	name string
}

func New(conf, backup *config.StorageConfig, monitor *stats.HandlerMonitor, info *livekit.EgressInfo) (*Uploader, error) {
	p, err := getUploader(conf)
	if err != nil {
		return nil, err
	}

	u := &Uploader{
		primary: p,
		monitor: monitor,
		info:    info,
	}

	if backup != nil {
		b, err := getUploader(backup)
		if err != nil {
			logger.Errorw("failed to create backup uploader", err)
		} else {
			u.backup = b
		}
		logger.Infow("creating backup uploader")
		u.BackupGlobal = s3.NewFromConfig(aws.Config{
			Region:       "us-east-1",
			Credentials:  credentials.NewStaticCredentialsProvider(backup.S3.AccessKey, backup.S3.Secret, ""),
			BaseEndpoint: aws.String(backup.S3.Endpoint),
		}, func(options *s3.Options) {
			options.UsePathStyle = true
		})
	}

	u.PrimaryGlobal = s3.NewFromConfig(aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(conf.S3.AccessKey, conf.S3.Secret, ""),
		BaseEndpoint: aws.String(conf.S3.Endpoint),
	}, func(options *s3.Options) {
		options.UsePathStyle = true
	})
	logger.Infow("creating primary uploader done")

	return u, nil
}

func getUploader(conf *config.StorageConfig) (*store, error) {
	if conf == nil {
		conf = &config.StorageConfig{}
	}

	var (
		s    storage.Storage
		err  error
		name string
	)
	switch {
	case conf.S3 != nil:
		s, err = storage.NewS3(conf.S3)
		name = "S3"
	case conf.GCP != nil:
		s, err = storage.NewGCP(conf.GCP)
		name = "GCP"
	case conf.Azure != nil:
		s, err = storage.NewAzure(conf.Azure)
		name = "Azure"
	case conf.AliOSS != nil:
		s, err = storage.NewAliOSS(conf.AliOSS)
		name = "AliOSS"
	default:
		s, err = storage.NewLocal(&storage.LocalConfig{})
		name = "Local"
	}
	if err != nil {
		return nil, err
	}

	return &store{
		Storage: s,
		conf:    conf,
		name:    name,
	}, nil
}

func uploadToS3(s *store, localFilepath string, storageFilepath string, outputType types.OutputType, uploader *s3.Client) (location string, size int64, err error) {
	conf := s.conf.S3

	for i := range 720 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		size, err = upload(ctx, conf.Bucket, localFilepath, storageFilepath, outputType, uploader)
		cancel()
		if err == nil {
			break
		}
		logger.Errorw("failed to upload to S3 in attempt "+strconv.Itoa(i)+" for file "+storageFilepath, err)
		time.Sleep(10 * time.Second)
	}
	if err != nil {
		return "", 0, errors.ErrUploadFailed(s.name, err)
	}

	return fmt.Sprintf("%s/%s/%s", conf.Endpoint, conf.Bucket, storageFilepath), size, nil
}

func upload(ctx context.Context, bucketName string, localPath string, remoteName string, outputType types.OutputType, uploader *s3.Client) (size int64, err error) {
	file, err := os.Open(localPath)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = file.Close()
	}()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	_, err = uploader.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucketName),
		Key:           aws.String(remoteName),
		Body:          file,
		ContentLength: aws.Int64(stat.Size()),
		ContentType:   aws.String(string(outputType)),
	})
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

func uploadToProvider(s *store, localFilepath string, storageFilepath string, outputType types.OutputType) (location string, size int64, err error) {
	storageFilepath = path.Join(s.conf.Prefix, storageFilepath)

	for i := range 720 {
		location, size, err = s.UploadFile(localFilepath, storageFilepath, string(outputType))
		if err != nil {
			fmt.Println(errors.ErrUploadFailed(s.name, err), "RETRYING UPLOAD: ", i)
		} else {
			break
		}
		time.Sleep(10 * time.Second)
	}
	if err != nil {
		return "", 0, errors.ErrUploadFailed(s.name, err)
	}

	if s.conf.GeneratePresignedUrl {
		location, err = s.GeneratePresignedUrl(storageFilepath, presignedExpiration)
		if err != nil {
			return "", 0, errors.ErrUploadFailed(s.name, err)
		}
	}

	return location, size, nil
}

func (u *Uploader) Upload(
	localFilepath, storageFilepath string,
	outputType types.OutputType,
	deleteAfterUpload bool,
) (string, int64, error) {

	var primaryErr error
	if !u.primaryFailed {
		start := time.Now()
		location, size, err := uploadToS3(u.primary, localFilepath, storageFilepath, outputType, u.PrimaryGlobal)
		elapsed := time.Since(start)

		if err == nil {
			if u.monitor != nil {
				u.monitor.IncUploadCountSuccess(string(outputType), float64(elapsed.Milliseconds()))
			}
			if deleteAfterUpload {
				_ = os.Remove(localFilepath)
			}
			return location, size, nil
		}
		if u.monitor != nil {
			u.monitor.IncUploadCountFailure(string(outputType), float64(elapsed.Milliseconds()))
		}
		u.primaryFailed = true
		primaryErr = err

	}

	if u.backup != nil {
		location, size, backupErr := uploadToS3(u.backup, localFilepath, storageFilepath, outputType, u.BackupGlobal)
		if backupErr == nil {
			if u.info != nil {
				u.info.SetBackupUsed()
			}
			if u.monitor != nil {
				u.monitor.IncBackupStorageWrites(string(outputType))
			}
			if deleteAfterUpload {
				_ = os.Remove(localFilepath)
			}
			return location, size, nil
		}

		if primaryErr != nil {
			return "", 0, psrpc.NewErrorf(psrpc.InvalidArgument,
				"primary: %s\nbackup: %s", primaryErr.Error(), backupErr.Error())
		}
		return "", 0, psrpc.NewError(psrpc.InvalidArgument, backupErr)
	}

	return "", 0, primaryErr
}

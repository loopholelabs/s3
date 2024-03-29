/*
	Copyright 2023 Loophole Labs

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		   http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog"
)

var (
	ErrDisabled = errors.New("s3 is disabled")
)

type Options struct {
	LogName   string
	Disabled  bool
	Endpoint  string
	Secure    bool
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
}

// S3 is a wrapper for the s3 client
type S3 struct {
	logger  *zerolog.Logger
	options *Options

	client     *minio.Client
	makeOpts   minio.MakeBucketOptions
	getOpts    minio.GetObjectOptions
	removeOpts minio.RemoveObjectOptions

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(options *Options, logger *zerolog.Logger) (*S3, error) {
	l := logger.With().Str(options.LogName, "S3").Logger()
	if options.Disabled {
		l.Warn().Msg("disabled")
		return nil, ErrDisabled
	}

	l.Debug().Msgf("connecting to s3 endpoint %s with bucket '%s'", options.Endpoint, options.Bucket)

	client, err := minio.New(options.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(options.AccessKey, options.SecretKey, ""),
		Secure: options.Secure,
		Region: options.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &S3{
		logger:     &l,
		options:    options,
		client:     client,
		makeOpts:   minio.MakeBucketOptions{},
		getOpts:    minio.GetObjectOptions{},
		removeOpts: minio.RemoveObjectOptions{},
		ctx:        ctx,
		cancel:     cancel,
	}

	return e, nil
}

func (e *S3) PresignedGetObject(ctx context.Context, prefix string, key string, expires time.Duration) (*url.URL, error) {
	objName := prefixedKey(prefix, key)
	e.logger.Debug().Msgf("presigning object '%s' from bucket '%s' with expiry %s", objName, e.options.Bucket, expires)
	return e.client.PresignedGetObject(ctx, e.options.Bucket, objName, expires, nil)
}

func (e *S3) GetObject(ctx context.Context, prefix string, key string) (io.ReadCloser, error) {
	objName := prefixedKey(prefix, key)
	e.logger.Debug().Msgf("getting object '%s' from bucket '%s'", objName, e.options.Bucket)
	return e.client.GetObject(ctx, e.options.Bucket, objName, e.getOpts)
}

func (e *S3) PutObject(ctx context.Context, prefix string, key string, reader io.Reader, objectSize int64, contentType string) (minio.UploadInfo, error) {
	objName := prefixedKey(prefix, key)
	e.logger.Debug().Msgf("putting object '%s' into bucket '%s'", objName, e.options.Bucket)
	return e.client.PutObject(ctx, e.options.Bucket, objName, reader, objectSize, minio.PutObjectOptions{
		ContentType: contentType,
	})
}

func (e *S3) DeleteObject(ctx context.Context, prefix string, key string) error {
	objName := prefixedKey(prefix, key)
	e.logger.Debug().Msgf("deleting object '%s' from bucket '%s'", objName, e.options.Bucket)
	return e.client.RemoveObject(ctx, e.options.Bucket, objName, e.removeOpts)
}

func (e *S3) MakeBucket(ctx context.Context, bucket string) error {
	e.logger.Debug().Msgf("making bucket '%s'", bucket)
	return e.client.MakeBucket(ctx, bucket, e.makeOpts)
}

func (e *S3) ListObjects(ctx context.Context, prefix string) <-chan minio.ObjectInfo {
	e.logger.Debug().Msgf("listing objects with prefix '%s' in bucket '%s'", prefix, e.options.Bucket)
	return e.client.ListObjects(ctx, e.options.Bucket, minio.ListObjectsOptions{
		Prefix: prefixedKey(prefix, ""),
	})
}

func (e *S3) RemoveBucket(ctx context.Context, bucket string) error {
	e.logger.Debug().Msgf("removing bucket '%s'", bucket)
	return e.client.RemoveBucket(ctx, bucket)
}

func (e *S3) Close() error {
	e.logger.Debug().Msg("closing s3 client")
	e.cancel()
	defer e.wg.Wait()
	return nil
}

func prefixedKey(prefix string, key string) string {
	return fmt.Sprintf("%s/%s", prefix, key)
}

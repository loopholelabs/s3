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
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog"
)

type Options struct {
	LogName   string
	Endpoint  string
	Secure    bool
	Region    string
	Prefix    string
	AccessKey string
	SecretKey string
}

// Client is a wrapper for the s3 client
type Client struct {
	logger     *zerolog.Logger
	options    *Options
	client     *minio.Client
	makeOpts   minio.MakeBucketOptions
	getOpts    minio.GetObjectOptions
	putOpts    minio.PutObjectOptions
	removeOpts minio.RemoveObjectOptions
	context    context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func New(options *Options, logger *zerolog.Logger) (*Client, error) {
	l := logger.With().Str(options.LogName, "S3").Logger()
	l.Debug().Msgf("connecting to s3 endpoint %s with bucket prefix '%s'", options.Endpoint, options.Prefix)

	client, err := minio.New(options.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(options.AccessKey, options.SecretKey, ""),
		Secure: options.Secure,
		Region: options.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &Client{
		logger:   &l,
		options:  options,
		client:   client,
		makeOpts: minio.MakeBucketOptions{},
		getOpts:  minio.GetObjectOptions{},
		putOpts: minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		},
		removeOpts: minio.RemoveObjectOptions{},
		context:    ctx,
		cancel:     cancel,
	}

	return e, nil
}

func (e *Client) PresignedGetObject(ctx context.Context, bucket string, key string, expires time.Duration) (*url.URL, error) {
	e.logger.Debug().Msgf("presigning object '%s' from bucket '%s' (prefix '%s') with expiry %s", key, bucket, e.options.Prefix, expires)
	return e.client.PresignedGetObject(ctx, e.options.Prefix+bucket, key, expires, nil)
}

func (e *Client) MakeBucket(ctx context.Context, bucket string) error {
	e.logger.Debug().Msgf("making bucket '%s' (prefix '%s')", bucket, e.options.Prefix)
	return e.client.MakeBucket(ctx, e.options.Prefix+bucket, e.makeOpts)
}

func (e *Client) RemoveBucket(ctx context.Context, bucket string) error {
	e.logger.Debug().Msgf("removing bucket '%s' (prefix '%s')", bucket, e.options.Prefix)
	return e.client.RemoveBucket(ctx, e.options.Prefix+bucket)
}

func (e *Client) GetObject(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
	e.logger.Debug().Msgf("getting object '%s' from bucket '%s' (prefix '%s')", key, bucket, e.options.Prefix)
	return e.client.GetObject(ctx, e.options.Prefix+bucket, key, e.getOpts)
}

func (e *Client) PutObject(ctx context.Context, bucket string, key string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
	e.logger.Debug().Msgf("putting object '%s' into bucket '%s' (prefix '%s')", key, bucket, e.options.Prefix)
	return e.client.PutObject(ctx, e.options.Prefix+bucket, key, reader, objectSize, e.putOpts)
}

func (e *Client) ListObjects(ctx context.Context, bucket string, subprefix string) <-chan minio.ObjectInfo {
	e.logger.Debug().Msgf("listing objects in bucket '%s' (prefix '%s') for subprefix %v", bucket, e.options.Prefix, subprefix)

	return e.client.ListObjects(ctx, e.options.Prefix+bucket, minio.ListObjectsOptions{
		Prefix: subprefix,
	})
}

func (e *Client) DeleteObject(ctx context.Context, bucket string, key string) error {
	e.logger.Debug().Msgf("deleting object '%s' from bucket '%s' (prefix '%s')", key, bucket, e.options.Prefix)
	return e.client.RemoveObject(ctx, e.options.Prefix+bucket, key, e.removeOpts)
}

func (e *Client) Close() error {
	e.logger.Debug().Msg("closing s3 client")
	e.cancel()
	defer e.wg.Wait()
	return nil
}

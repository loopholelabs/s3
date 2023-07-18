/*
 * 	Copyright 2023 Loophole Labs
 *
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 		   http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 */

package config

import (
	"errors"
	"github.com/loopholelabs/s3"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	ErrEndpointRequired  = errors.New("endpoint is required")
	ErrRegionRequired    = errors.New("region is required")
	ErrBucketRequired    = errors.New("bucket is required")
	ErrAccessKeyRequired = errors.New("access key is required")
	ErrSecretKeyRequired = errors.New("secret key is required")
)

const (
	DefaultSecure = true
	DefaultRegion = "auto"
)

type Config struct {
	Endpoint  string `yaml:"endpoint"`
	Secure    bool   `yaml:"secure"`
	Region    string `yaml:"region"`
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

func New() *Config {
	return &Config{
		Secure: DefaultSecure,
		Region: DefaultRegion,
	}
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return ErrEndpointRequired
	}

	if c.Region == "" {
		return ErrRegionRequired
	}

	if c.Bucket == "" {
		return ErrBucketRequired
	}

	if c.AccessKey == "" {
		return ErrAccessKeyRequired
	}

	if c.SecretKey == "" {
		return ErrSecretKeyRequired
	}

	return nil
}

func (c *Config) RootPersistentFlags(flags *pflag.FlagSet) {
	flags.StringVar(&c.Endpoint, "s3-endpoint", "", "The s3 endpoint")
	flags.BoolVar(&c.Secure, "s3-secure", DefaultSecure, "The s3 secure flag")
	flags.StringVar(&c.Region, "s3-region", DefaultRegion, "The s3 region")
	flags.StringVar(&c.Bucket, "s3-bucket", "", "The s3 bucket to use")
	flags.StringVar(&c.AccessKey, "s3-access-key", "", "The s3 access key")
	flags.StringVar(&c.SecretKey, "s3-secret-key", "", "The s3 secret key")
}

func (c *Config) GlobalRequiredFlags(cmd *cobra.Command) error {
	err := cmd.MarkFlagRequired("s3-endpoint")
	if err != nil {
		return err
	}

	err = cmd.MarkFlagRequired("s3-bucket")
	if err != nil {
		return err
	}

	err = cmd.MarkFlagRequired("s3-access-key")
	if err != nil {
		return err
	}

	err = cmd.MarkFlagRequired("s3-secret-key")
	if err != nil {
		return err
	}

	return nil
}

func (c *Config) GenerateOptions(logName string) *s3.Options {
	return &s3.Options{
		LogName:   logName,
		Secure:    c.Secure,
		Region:    c.Region,
		Endpoint:  c.Endpoint,
		Bucket:    c.Bucket,
		AccessKey: c.AccessKey,
		SecretKey: c.SecretKey,
	}
}

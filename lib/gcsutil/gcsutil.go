// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package gcsutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// Reports whether err indicates the requested resource was not found.
func IsNotExist(err error) bool {
	return err == storage.ErrObjectNotExist || os.IsNotExist(err)
}

// Client is a wrapper for a GCS client that provides basic read and write
// facilities to a specific bucket.
type Client struct {
	Client *storage.Client
	Bucket string
}

// NewClient creates a new GCS client for the specified bucket.
func NewClient(ctx context.Context, bucket string, opts ...option.ClientOption) (*Client, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &Client{
		Client: client,
		Bucket: bucket,
	}, nil
}

// Put writes the specified data to GCS under the given key.
func (c *Client) Put(ctx context.Context, key string, data io.Reader) error {
	wc := c.Client.Bucket(c.Bucket).Object(key).NewWriter(ctx)

	// Copy the data to the writer.
	if _, err := io.Copy(wc, data); err != nil {
		wc.Close()
		return fmt.Errorf("write to GCS: %w", err)
	}

	// Close the writer.
	if err := wc.Close(); err != nil {
		return fmt.Errorf("close GCS writer: %w", err)
	}
	return nil
}

// Get returns the contents of the specified key from GCS.
// If the key is not found, the resulting error satisfies fs.ErrNotExist.
func (c *Client) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	// Get the object.
	obj := c.Client.Bucket(c.Bucket).Object(key)

	// Check if object exists.
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		// If the object does not exist, return fs.ErrNotExist.
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, -1, fmt.Errorf("key %q: %w", key, fs.ErrNotExist)
		}
		// Otherwise, return the error.
		return nil, -1, err
	}

	// Otherwise, return the reader and the size.
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, -1, err
	}
	return r, attrs.Size, nil
}

// GetData returns the contents of the specified key from GCS as a byte slice.
func (c *Client) GetData(ctx context.Context, key string) ([]byte, error) {
	r, _, err := c.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// PutCond writes the specified data to GCS under the given key if the key does
// not already exist, or if its content differs from the given md5 hash.
// The md5 hash is an MD5 of the expected contents, encoded as lowercase hex digits.
func (c *Client) PutCond(ctx context.Context, key, md5hash string, data io.Reader) (written bool, _ error) {
	// Get the object.
	obj := c.Client.Bucket(c.Bucket).Object(key)

	// Check if object exists.
	attrs, err := obj.Attrs(ctx)
	if err == nil && attrs.MD5 != nil {
		// Object exists, check if MD5 matches
		existingMD5 := fmt.Sprintf("%x", attrs.MD5)
		if existingMD5 == md5hash {
			return false, nil
		}
	} else if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		// Some error other than "object does not exist" occurred.
		return false, err
	}

	// Object does not exist, write it.
	if err := c.Put(ctx, key, data); err != nil {
		return false, err
	}
	// Object was written.
	return true, nil
}

// Close closes the GCS client.
func (c *Client) Close() error {
	return c.Client.Close()
}

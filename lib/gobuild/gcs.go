// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package gobuild

import (
	"context"
	"crypto/md5"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/creachadair/gocache"
	"github.com/creachadair/gocache/cachedir"
	"github.com/creachadair/taskgroup"
	"github.com/tailscale/go-cache-plugin/lib/gcsutil"
)

// GCSCache is a cache implementation that stores cached files in a local directory and
// also stores cached files in a remote GCS bucket.
type GCSCache struct {
	// Local is the local cache directory where actions and objects are staged.
	// It must be non-nil. A local stage is required because the Go toolchain
	// needs direct access to read the files reported by the cache
	Local *cachedir.Dir

	// GCS is the remote GCS bucket where actions and objects are stored.
	// It must be non-nil.
	GCSClient *gcsutil.Client

	// KeyPrefix is the prefix used for keys in the GCS bucket.
	KeyPrefix string

	// MinUploadSize is the minimum size of a file to upload to GCS.
	MinUploadSize int64

	// UploadConcurrency is the number of concurrent uploads to GCS.
	UploadConcurrency int

	// Tracks tasks pushing cache writes to GCS
	initOnce sync.Once
	push     *taskgroup.Group
	start    func(taskgroup.Task)

	// Metrics
	getLocalHit  expvar.Int // count of Get hits in the local cache
	getFaultHit  expvar.Int // count of Get hits faulted in from GCS
	getFaultMiss expvar.Int // count of Get faults that were misses
	putSkipSmall expvar.Int // count of "small" objects not written to GCS
	putGCSFound  expvar.Int // count of objects not written to GCS because they were already present
	putGCSAction expvar.Int // count of actions written to GCS
	putGCSObject expvar.Int // count of objects written to GCS
	putGCSError  expvar.Int // count of errors writing to GCS
}

// init initializes the GCSCache.
func (s *GCSCache) init() {
	// Initialize the push group.
	// Only executes once.
	s.initOnce.Do(func() {
		s.push, s.start = taskgroup.New(nil).Limit(s.uploadConcurrency())
	})
}

// uploadConcurrency returns the number of concurrent uploads to GCS.
func (s *GCSCache) uploadConcurrency() int {
	if s.UploadConcurrency > 0 {
		return s.UploadConcurrency
	}
	return runtime.NumCPU()
}

func (s *GCSCache) Get(ctx context.Context, actionID string) (outputId, diskPath string, _ error) {
	// Initialize the GCSCache.
	s.init()

	// Try to get the object from the local cache.
	objID, dPath, err := s.Local.Get(ctx, actionID)
	if err == nil && objID != "" && dPath != "" {
		s.getLocalHit.Add(1)
		// cache hit, return the object ID and disk path.
		return objID, dPath, nil
	}

	// Either we failed to get the object from the local cache, or the object does not exist.
	// Try to get the object from the remote GCS bucket.
	action, err := s.GCSClient.GetData(ctx, s.actionKey(actionID))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			s.getFaultMiss.Add(1)
			return "", "", nil
		}
		return "", "", fmt.Errorf("[gcs] read action %s: %w", actionID, err)
	}

	// We got an action hit remotely, try to update the local copy.
	outputID, mtime, err := parseAction(action)
	if err != nil {
		return "", "", err
	}

	object, size, err := s.GCSClient.Get(ctx, s.outputKey(outputID))
	if err != nil {
		// At this point we know the action exists, so if we can't read the
		// object report it as an error rather than a cache miss.
		return "", "", fmt.Errorf("[gcs] read object %s: %w", outputID, err)
	}
	defer object.Close()
	s.getFaultHit.Add(1)

	// Now we should have the body; poke it into the local cache.  Preserve the
	// modification timestamp recorded with the original action.
	diskPath, err = s.Local.Put(ctx, gocache.Object{
		ActionID: actionID,
		OutputID: outputID,
		Size:     size,
		Body:     object,
		ModTime:  mtime,
	})
	return outputID, diskPath, err
}

// Put implements the corresponding callback of the cache protocol.
func (s *GCSCache) Put(ctx context.Context, obj gocache.Object) (diskPath string, _ error) {
	s.init()

	// Compute an MD5 hash so we can do a conditional put on the object data.
	h := md5.New()
	obj.Body = io.TeeReader(obj.Body, h)

	diskPath, err := s.Local.Put(ctx, obj)
	if err != nil {
		return "", err // don't bother trying to forward it to the remote
	}
	if obj.Size < s.MinUploadSize {
		s.putSkipSmall.Add(1)
		return diskPath, nil
	}

	// Make a file copy of the output data to write to GCS. At this point we've
	// already computed the hash (above) and written the data to the local cache,
	// so we can afford to be a bit lazy here.
	f, err := os.Open(diskPath)
	if err != nil {
		return diskPath, nil
	}

	// Don't hold up the caller waiting for GCS to be updated; the data are safe in
	// the local cache.
	actionID, outputID := obj.ActionID, obj.OutputID
	hash := fmt.Sprintf("%x", h.Sum(nil))

	// Ctx is from the outer function signature: func (s *GCSCache) Put(ctx context.Context, ...).
	bgctx := context.Background()

	s.start(func() error {
		// Give the background task its own timeout, like in S3 code.
		sctx, cancel := context.WithTimeout(bgctx, 1*time.Minute)
		defer cancel()

		data := fmt.Sprintf("%s %d", outputID, obj.ModTime.Unix())
		if _, err := s.GCSClient.PutCond(sctx, s.actionKey(actionID), "", strings.NewReader(data)); err != nil {
			s.putGCSError.Add(1)
			return fmt.Errorf("[gcs] write action %s: %w", actionID, err)
		}
		s.putGCSAction.Add(1)

		if ok, err := s.GCSClient.PutCond(sctx, s.outputKey(outputID), hash, f); err != nil {
			s.putGCSError.Add(1)
			return fmt.Errorf("[gcs] write object %s: %w", outputID, err)
		} else if !ok {
			s.putGCSFound.Add(1)
		} else {
			s.putGCSObject.Add(1)
		}
		f.Close()
		return nil
	})
	return diskPath, nil
}

// SetMetrics publishes the metrics collected by s to r.
func (s *GCSCache) SetMetrics(_ context.Context, r *expvar.Map) {
	r.Set("get_local_hit", &s.getLocalHit)
	r.Set("get_fault_hit", &s.getFaultHit)
	r.Set("get_fault_miss", &s.getFaultMiss)
	r.Set("put_skip_small", &s.putSkipSmall)
	r.Set("put_gcs_found", &s.putGCSFound)
	r.Set("put_gcs_action", &s.putGCSAction)
	r.Set("put_gcs_object", &s.putGCSObject)
	r.Set("put_gcs_error", &s.putGCSError)
}

// actionKey constructs the GCS key path for an action ID.
func (s *GCSCache) actionKey(actionID string) string {
	key := path.Join("action", actionID[:2], actionID[2:])
	if s.KeyPrefix != "" {
		key = path.Join(s.KeyPrefix, key)
	}
	return key
}

// outputKey constructs the GCS key path for an output ID.
func (s *GCSCache) outputKey(outputID string) string {
	key := path.Join("object", outputID[:2], outputID[2:])
	if s.KeyPrefix != "" {
		key = path.Join(s.KeyPrefix, key)
	}
	return key
}

// Close implements the corresponding callback of the cache protocol.
func (s *GCSCache) Close(ctx context.Context) error {
	s.init()
	s.push.Wait() // wait for any remaining uploads to complete

	if err := s.GCSClient.Close(); err != nil {
		return fmt.Errorf("[gcs] close client: %w", err)
	}
	return nil
}

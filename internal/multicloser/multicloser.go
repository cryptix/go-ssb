// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package multicloser

import (
	"fmt"
	"io"
	"sync"

	multierror "github.com/hashicorp/go-multierror"
)

type MultiCloser struct {
	cs []io.Closer
	l  sync.Mutex
}

func (mc *MultiCloser) AddCloser(c io.Closer) {
	mc.l.Lock()
	defer mc.l.Unlock()

	mc.cs = append(mc.cs, c)
}

var _ io.Closer = (*MultiCloser)(nil)

func (mc *MultiCloser) Close() error {
	mc.l.Lock()
	defer mc.l.Unlock()

	var (
		hasErrs bool
		err     error
	)

	// Close in reverse order (LIFO) so that resources opened first
	// (like databases) are closed last, after their dependents.
	for i := len(mc.cs) - 1; i >= 0; i-- {
		if cerr := mc.cs[i].Close(); cerr != nil {
			err = multierror.Append(err, fmt.Errorf("multiCloser: c%d failed: %w", i, cerr))
			hasErrs = true
		}
	}

	if !hasErrs {
		return nil
	}

	return err
}

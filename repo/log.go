// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package repo

import (
	"fmt"

	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/margaret/v2/offset2"
)

func OpenLog(r Interface, path ...string) (*multimsg.WrappedLog, error) {
	// prefix path with "logs" if path is not empty, otherwise use "log"
	path = append([]string{"log"}, path...)
	if len(path) > 1 {
		path[0] = "logs"
	}

	log, err := offset2.Open[*multimsg.MultiMessage](r.GetPath(path...))
	if err != nil {
		return nil, fmt.Errorf("failed to open log: %w", err)
	}
	return multimsg.NewWrappedLog(log), nil
}

// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/offset2"

	"github.com/ssbc/go-ssb/message/multimsg"
)

func main() {
	var dryRun bool
	flag.BoolVar(&dryRun, "dry", false, "only output what it would do")

	var limit int
	flag.IntVar(&limit, "limit", -1, "how many entries to copy (defaults to unlimited)")

	flag.Parse()

	logPaths := flag.Args()
	if len(logPaths) != 2 {
		cmdName := os.Args[0]
		fmt.Fprintf(os.Stderr, "usage: %s <options> <input path> <output path>\n", cmdName)
		os.Exit(1)
	}

	if limit == -1 {
		fmt.Fprintf(os.Stderr, "warning: nothing to do without a limit. exiting\n")
		os.Exit(1)
	}

	if dryRun {
		fmt.Fprintf(os.Stderr, "would copy offset2 log\n")
		fmt.Fprintf(os.Stderr, "locations %s to %s\n", logPaths[0], logPaths[1])
		return
	}

	input, err := offset2.Open[*multimsg.MultiMessage](logPaths[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open input log %s: %s\n", logPaths[0], err)
		os.Exit(1)
	}

	output, err := offset2.Open[*multimsg.MultiMessage](logPaths[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open output log %s: %s\n", logPaths[1], err)
		os.Exit(1)
	}

	qry := input.Query(margaret.Limit(limit))
	for _, mm := range qry.Iter() {
		_, err = output.Append(mm)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write entry to output log %s: %s\n", logPaths[1], err)
			os.Exit(1)
		}
	}
	if err := qry.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to read input log %s: %s\n", logPaths[0], err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "all done. closing output log.")

	if c, ok := any(output).(io.Closer); ok {
		if err = c.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close output log %s: %s\n", logPaths[1], err)
		}
	}
}

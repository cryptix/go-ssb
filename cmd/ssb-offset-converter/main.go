// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	margaret "github.com/ssbc/margaret/v2"
	"github.com/ssbc/margaret/v2/legacyflumeoffset"
	"github.com/ssbc/margaret/v2/offset2"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/message/multimsg"
)

func main() {
	var dryRun bool
	flag.BoolVar(&dryRun, "dry", false, "only output what it would do")

	var limit int
	flag.IntVar(&limit, "limit", -1, "how many entries to copy (defaults to unlimited)")

	var inputFormat string
	flag.StringVar(&inputFormat, "if", "", "input format: lfo (legacy flume offset) or empty for offset2")

	flag.Parse()

	logPaths := flag.Args()
	if len(logPaths) != 2 {
		cmdName := os.Args[0]
		fmt.Fprintf(os.Stderr, "usage: %s <options> <input path> <output path>\n", cmdName)
		os.Exit(1)
	}

	if dryRun {
		fmt.Fprintf(os.Stderr, "would copy log\n")
		fmt.Fprintf(os.Stderr, "locations %s to %s\n", logPaths[0], logPaths[1])
		return
	}

	output, err := offset2.Open[*multimsg.MultiMessage](logPaths[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open output log %s: %s\n", logPaths[1], err)
		os.Exit(1)
	}

	switch inputFormat {
	case "lfo":
		convertFromLegacyFlume(logPaths[0], output, limit)
	case "":
		convertFromOffset2(logPaths[0], output, limit)
	default:
		fmt.Fprintf(os.Stderr, "unknown input format: %s\n", inputFormat)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "all done. closing output log.")

	if c, ok := any(output).(io.Closer); ok {
		if err = c.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close output log %s: %s\n", logPaths[1], err)
		}
	}
}

// kvtRawValue is used to extract the raw "value" JSON from a KVT envelope.
type kvtRawValue struct {
	Value json.RawMessage `json:"value"`
}

func convertFromLegacyFlume(inputPath string, output margaret.Log[*multimsg.MultiMessage], limit int) {
	reader, err := legacyflumeoffset.OpenReadOnly(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open input log %s: %s\n", inputPath, err)
		os.Exit(1)
	}
	defer reader.Close()

	count := 0
	for raw, err := range reader.ReadAll() {
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read entry %d: %s\n", count, err)
			os.Exit(1)
		}

		// Parse the full KVT envelope for structured access.
		var kvr refs.KeyValueRaw
		if err := json.Unmarshal(raw, &kvr); err != nil {
			fmt.Fprintf(os.Stderr, "failed to parse KVT entry %d: %s\n", count, err)
			os.Exit(1)
		}

		// Extract the raw "value" JSON to preserve original bytes.
		var env kvtRawValue
		if err := json.Unmarshal(raw, &env); err != nil {
			fmt.Fprintf(os.Stderr, "failed to extract raw value for entry %d: %s\n", count, err)
			os.Exit(1)
		}

		mm := multimsg.NewMultiMessageFromKeyValRaw(kvr, env.Value)
		if _, err := output.Append(&mm); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write entry %d: %s\n", count, err)
			os.Exit(1)
		}

		count++
		if limit >= 0 && count >= limit {
			break
		}
	}

	fmt.Fprintf(os.Stderr, "converted %d entries from legacy flume offset\n", count)
}

func convertFromOffset2(inputPath string, output margaret.Log[*multimsg.MultiMessage], limit int) {
	input, err := offset2.Open[*multimsg.MultiMessage](inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open input log %s: %s\n", inputPath, err)
		os.Exit(1)
	}

	var opts []margaret.QueryOption
	if limit >= 0 {
		opts = append(opts, margaret.Limit(limit))
	}

	qry := input.Query(opts...)
	count := 0
	for _, mm := range qry.Iter() {
		if _, err := output.Append(mm); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write entry %d: %s\n", count, err)
			os.Exit(1)
		}
		count++
	}
	if err := qry.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to read input log: %s\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "copied %d entries from offset2\n", count)
}

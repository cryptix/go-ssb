// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"

	margaret "github.com/ssbc/margaret/v2"

	"github.com/ssbc/go-ssb/repo"
)

func check(err error) {
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err)
	fmt.Fprintln(os.Stderr, "occurred at")
	debug.PrintStack()
	os.Exit(1)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: ssb-logcat <repo> <startSeq> <limit>")
		os.Exit(1)
	}
	fromPath := os.Args[1]

	startSeqInt, err := strconv.Atoi(os.Args[2])
	check(err)
	startSeq := int64(startSeqInt)

	limit, err := strconv.Atoi(os.Args[3])
	check(err)

	repoFrom := repo.New(fromPath)

	from, err := repo.OpenLog(repoFrom)
	check(err)

	qry := from.Query(margaret.Gt(startSeq), margaret.Limit(limit))
	for seq, mm := range qry.Iter() {
		if mm.Message == nil {
			continue
		}
		msg := mm.Message
		os.Stdout.WriteString(fmt.Sprintf(`
		{
			"key": %q,
			"rxSeq": %d,
			"value":
		`, msg.Key().String(), seq))
		os.Stdout.Write(msg.ValueContentJSON())
		os.Stdout.WriteString("}\n")
	}
	check(qry.Err())
}

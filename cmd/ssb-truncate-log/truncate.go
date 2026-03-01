// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"time"

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
		fmt.Fprintln(os.Stderr, "usage: migrate3 <from> <to> <limit>")
		os.Exit(1)
	}
	fromPath := os.Args[1]
	toPath := os.Args[2]

	limit, err := strconv.Atoi(os.Args[3])
	check(err)

	repoFrom := repo.New(fromPath)
	repoTo := repo.New(toPath)

	from, err := repo.OpenLog(repoFrom)
	check(err)

	to, err := repo.OpenLog(repoTo)
	check(err)

	fmt.Println("element count in source log:", from.Seq())
	start := time.Now()

	qry := from.Query(margaret.Limit(limit))
	var seq int64
	for _, mm := range qry.Iter() {
		seq, err = to.Append(mm)
		fmt.Print("\r", seq)
		if err != nil {
			check(err)
		}
	}
	check(qry.Err())

	fmt.Println()
	fmt.Println("copy done after:", time.Since(start))

	fmt.Println("target has", to.Seq())
}

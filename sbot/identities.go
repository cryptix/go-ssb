// SPDX-FileCopyrightText: 2021 The Go-SSB Authors
//
// SPDX-License-Identifier: MIT

package sbot

import (
	"fmt"

	refs "github.com/ssbc/go-ssb-refs"
	"github.com/ssbc/go-ssb/message"
	"github.com/ssbc/go-ssb/message/multimsg"
	"github.com/ssbc/go-ssb/repo"
)

func (sbot *Sbot) PublishAs(nick string, val interface{}) (refs.Message, error) {
	r := repo.New(sbot.repoPath)

	kp, err := repo.LoadKeyPair(r, nick)
	if err != nil {
		return nil, err
	}

	var pubopts = []message.PublishOption{
		message.UseNowTimestamps(true),
		message.UseWaitForIndexesCallback(sbot.WaitUntilIndexesAreSynced),
	}
	if sbot.signHMACsecret != nil { // all feeds use the same settings right now
		pubopts = append(pubopts, message.SetHMACKey(sbot.signHMACsecret))
	}

	rxlog, ok := sbot.ReceiveLog.(*multimsg.WrappedLog)
	if !ok {
		return nil, fmt.Errorf("publishAs: unexpected receive log type %T", sbot.ReceiveLog)
	}
	pl, err := message.OpenPublishLog(rxlog, sbot.Users, kp, pubopts...)
	if err != nil {
		return nil, fmt.Errorf("publishAs: failed to create publish log: %w", err)
	}

	return pl.Publish(val)
}

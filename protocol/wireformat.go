// Copyright (C) 2014 Jakob Borg and other contributors. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

package protocol

import (
	"path/filepath"

	"code.google.com/p/go.text/unicode/norm"
)

type wireFormatConnection struct {
	next Connection
}

func (c wireFormatConnection) ID() NodeID {
	return c.next.ID()
}

func (c wireFormatConnection) Index(repo string, fs []FileInfo) {
	for i := range fs {
		fs[i].Name = norm.NFC.String(filepath.ToSlash(fs[i].Name))
	}

	c.next.Index(repo, fs)
}

func (c wireFormatConnection) IndexUpdate(repo string, fs []FileInfo) {
	for i := range fs {
		fs[i].Name = norm.NFC.String(filepath.ToSlash(fs[i].Name))
	}

	c.next.IndexUpdate(repo, fs)
}

func (c wireFormatConnection) Request(repo, name string, offset int64, size int) ([]byte, error) {
	name = norm.NFC.String(filepath.ToSlash(name))
	return c.next.Request(repo, name, offset, size)
}

func (c wireFormatConnection) ClusterConfig(config ClusterConfigMessage) {
	c.next.ClusterConfig(config)
}

func (c wireFormatConnection) Statistics() Statistics {
	return c.next.Statistics()
}

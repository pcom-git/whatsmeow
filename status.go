// Copyright (c) 2021 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func (cli *Client) handleStatus(ctx context.Context, node *waBinary.Node) {
	var cancelled bool
	defer cli.maybeDeferredAck(ctx, node)(&cancelled)
}

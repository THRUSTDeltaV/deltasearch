package batchinggetter

import (
	"context"
	"strings"

	"github.com/opensearch-project/opensearch-go"
)

type batch map[string]bulkRequest

func getKey(rr reqresp) string {
	return strings.Join(rr.req.Fields, "") + rr.req.Index
}

func (b batch) add(rr reqresp) {
	b[getKey(rr)][rr.req.DocumentID] = rr
}

func (b batch) execute(ctx context.Context, client *opensearch.Client) error {
	for _, br := range b {
		if err := br.performBulkRequest(ctx, client); err != nil {
			// Note: this will terminate batch on first error in request.
			return err
		}
	}

	return nil
}

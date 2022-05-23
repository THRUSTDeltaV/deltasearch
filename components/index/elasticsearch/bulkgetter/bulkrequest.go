package bulkgetter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

// ErrHTTP represents non-404 errors in HTTP requests.
var ErrHTTP = errors.New("HTTP Error")

type bulkRequest map[string]reqresp

func newBulkRequest(size int) bulkRequest {
	return make(bulkRequest, size)
}

func (r bulkRequest) sendBulkResponse(found bool, err error) {
	for _, rr := range r {
		rr.resp <- GetResponse{found, err}
		close(rr.resp)
		// Note that this does not do delete() as it should become irrelevant/unnecessary here.
	}
}

type responseDoc struct {
	Index  string          `json:"_index"`
	ID     string          `json:"_id"`
	Found  bool            `json:"found"`
	Source json.RawMessage `json:"_source"`
}

func keyFromResponseDoc(doc responseDoc) string {
	return doc.Index + doc.ID
}

func keyFromRR(rr reqresp) string {
	return rr.req.Index + rr.req.DocumentID
}

func (r bulkRequest) add(rr reqresp) {
	r[keyFromRR(rr)] = rr
}

func (r bulkRequest) sendResponse(key string, found bool, err error) {
	rr := r[key]
	rr.resp <- GetResponse{found, err}
	close(rr.resp)
	delete(r, key) // Is delete the best way to do this, or setting to nil?
}

func (r bulkRequest) getReqBody() io.Reader {
	// Example
	// {
	//   "docs": [
	//   {
	//     "_index": "sample-index1",
	//     "_id": "1"
	//   },
	//   {
	//     "_index": "sample-index2",
	//     "_id": "1",
	//     "_source": {
	//       "include": ["Length"]
	//     }
	//   }
	//   ]
	// }

	type source struct {
		Include []string `json:"include"`
	}

	type doc struct {
		Index  string `json:"_index"`
		ID     string `json:"_id"`
		Source source `json:"_source"`
	}

	docs := make([]doc, len(r))

	i := 0
	for _, rr := range r {
		docs[i] = doc{
			Index: rr.req.Index,
			ID:    rr.req.DocumentID,
			Source: source{
				rr.req.Fields,
			},
		}

		i++
	}

	bodyStruct := struct {
		Docs []doc `json:"docs"`
	}{docs}

	var buffer bytes.Buffer

	e := json.NewEncoder(io.Writer(&buffer))
	e.Encode(bodyStruct)

	return io.Reader(&buffer)
}

func (r bulkRequest) getRequest() *opensearchapi.MgetRequest {
	body := r.getReqBody()

	trueConst := true

	req := opensearchapi.MgetRequest{
		Body:       body,
		Preference: "_local",
		Realtime:   &trueConst,
	}

	return &req
}

func decodeResponse(res *opensearchapi.Response) ([]responseDoc, error) {
	response := struct {
		Docs []responseDoc `json:"docs"`
	}{}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, err
	}

	return response.Docs, nil
}

func (r bulkRequest) processResponse(res *opensearchapi.Response) error {
	// Example response
	// {
	//   "docs": [
	//     {
	//       "_index": "sample-index1",
	//       "_type": "_doc",
	//       "_id": "1",
	//       "_version": 4,
	//       "_seq_no": 5,
	//       "_primary_term": 19,
	//       "found": true,
	//       "_source": {
	//         "Title": "Batman Begins",
	//         "Director": "Christopher Nolan"
	//       }
	//     },
	//     {
	//       "_index": "sample-index2",
	//       "_type": "_doc",
	//       "_id": "1",
	//       "_version": 1,
	//       "_seq_no": 6,
	//       "_primary_term": 19,
	//       "found": true,
	//       "_source": {
	//         "Title": "The Dark Knight",
	//         "Director": "Christopher Nolan"
	//       }
	//     }
	//   ]
	// }

	var err error

	switch res.StatusCode {
	case 200:
		// Found

		docs, err := decodeResponse(res)
		if err != nil {
			err = fmt.Errorf("error decoding body: %w", err)
			r.sendBulkResponse(false, err)
			return err
		}

		for _, d := range docs {
			key := keyFromResponseDoc(d)

			if d.Found == true {
				if err = json.Unmarshal(d.Source, r[key].dst); err != nil {
					err = fmt.Errorf("error decoding source: %w", err)
					r.sendResponse(key, false, err)
					return err
				}

				r.sendResponse(key, true, nil)
			} else {
				r.sendResponse(key, false, nil)
			}
		}

	default:
		if res.IsError() {
			err = fmt.Errorf("%w: %s", ErrHTTP, res)
		} else {
			err = fmt.Errorf("Unexpected HTTP return code: %d", res.StatusCode)
		}
	}

	r.sendBulkResponse(false, err)
	return err
}

func (r bulkRequest) execute(ctx context.Context, client *opensearch.Client) error {
	log.Printf("Performing bulk GET, %d elements", len(r))

	res, err := r.getRequest().Do(ctx, client)
	if err != nil {
		r.sendBulkResponse(false, err)
		return err
	}

	defer res.Body.Close()

	if err = r.processResponse(res); err != nil {
		return err
	}

	return nil
}

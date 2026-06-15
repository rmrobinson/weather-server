package ingester

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

type livedataResponse struct {
	CommonList []livedataEntry `json:"common_list"`
}

type livedataEntry struct {
	ID  string `json:"id"`
	Val string `json:"val"`
}

// fetchUVIndex calls the GW2000B local API and returns the UV index from field "0x17".
// Returns (value, true) on success; (0, false) on any error or if the field is absent.
func fetchUVIndex(ctx context.Context, client *http.Client, baseURL string) (float64, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/get_livedata_info", nil)
	if err != nil {
		return 0, false
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	var data livedataResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, false
	}

	for _, e := range data.CommonList {
		if e.ID == "0x17" {
			v, err := strconv.ParseFloat(strings.TrimSpace(e.Val), 64)
			if err != nil {
				return 0, false
			}
			return v, true
		}
	}
	return 0, false
}

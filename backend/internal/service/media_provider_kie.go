package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type MediaURL struct {
	Kind string `json:"kind"`
	URL  string `json:"url"`
}
type MediaProviderResult struct {
	ProviderTaskID string     `json:"provider_task_id,omitempty"`
	Status         string     `json:"status"`
	URLs           []MediaURL `json:"urls"`
	Title          string     `json:"title,omitempty"`
	CoverURL       string     `json:"cover_url,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
}
type mediaProvider interface {
	Create(context.Context, string, map[string]any, *AuthorizationHandle) (string, bool, error)
	Read(context.Context, string, string) (MediaProviderResult, error)
}
type kieMediaProvider struct {
	upstream        HTTPUpstream
	baseURL, apiKey string
	accountID       int64
}
type kieEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"msg"`
	Data    json.RawMessage `json:"data"`
}

func (p *kieMediaProvider) call(ctx context.Context, method, path string, body []byte) (kieEnvelope, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(p.baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return kieEnvelope{}, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.upstream.Do(req, "", p.accountID, 4)
	if err != nil {
		return kieEnvelope{}, 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return kieEnvelope{}, resp.StatusCode, err
	}
	var envelope kieEnvelope
	err = json.Unmarshal(raw, &envelope)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return envelope, resp.StatusCode, fmt.Errorf("provider status %d", resp.StatusCode)
	}
	if err != nil {
		return envelope, resp.StatusCode, errors.New("provider response could not be decoded")
	}
	return envelope, resp.StatusCode, nil
}

func (p *kieMediaProvider) Create(ctx context.Context, model string, input map[string]any, h *AuthorizationHandle) (string, bool, error) {
	body, err := json.Marshal(map[string]any{"model": mediaUpstreamModel(model), "input": input})
	if err != nil {
		return "", true, err
	}
	envelope, status, err := p.call(WithAuthorizationHandle(ctx, h), http.MethodPost, "/api/v1/jobs/createTask", body)
	// A server/transport timeout cannot prove that no task was accepted. Only
	// explicit request rejection is refundable; unknown writes are never retried.
	rejected := status >= 400 && status < 500 && status != 408
	if err != nil {
		return "", rejected, err
	}
	if envelope.Code != 200 {
		return "", envelope.Code >= 400 && envelope.Code < 500 && envelope.Code != 408, fmt.Errorf("provider rejected request (%d)", envelope.Code)
	}
	var data struct {
		TaskID string `json:"taskId"`
	}
	if json.Unmarshal(envelope.Data, &data) != nil || strings.TrimSpace(data.TaskID) == "" {
		return "", false, errors.New("provider response has no task identity")
	}
	return data.TaskID, false, nil
}

func (p *kieMediaProvider) Read(ctx context.Context, taskID, kind string) (MediaProviderResult, error) {
	envelope, _, err := p.call(WithNonBillableUpstream(ctx, NonBillableMediaFetch), http.MethodGet, "/api/v1/jobs/recordInfo?taskId="+url.QueryEscape(taskID), nil)
	if err != nil {
		return MediaProviderResult{}, err
	}
	if envelope.Code != 200 {
		return MediaProviderResult{}, fmt.Errorf("provider query failed (%d)", envelope.Code)
	}
	var record struct {
		TaskID      string          `json:"taskId"`
		State       string          `json:"state"`
		ResultJSON  json.RawMessage `json:"resultJson"`
		FailCode    json.RawMessage `json:"failCode"`
		FailMessage string          `json:"failMsg"`
		Response    struct {
			Data []struct {
				AudioURL       string `json:"audio_url"`
				StreamAudioURL string `json:"stream_audio_url"`
				Title          string `json:"title"`
				ImageURL       string `json:"image_url"`
			} `json:"data"`
		} `json:"response"`
	}
	if err := json.Unmarshal(envelope.Data, &record); err != nil || record.State == "" || record.TaskID != taskID {
		return MediaProviderResult{}, errors.New("provider query identity or state invalid")
	}
	r := MediaProviderResult{ProviderTaskID: taskID, URLs: []MediaURL{}}
	switch record.State {
	case "waiting", "queuing":
		r.Status = "pending"
	case "generating":
		r.Status = "processing"
	case "success":
		r.Status = "success"
	case "fail":
		r.Status = "failed"
		r.ErrorCode = strings.Trim(string(record.FailCode), "\"")
		r.ErrorMessage = record.FailMessage
	default:
		return MediaProviderResult{}, errors.New("provider query state is unrecognized")
	}
	if r.Status != "success" {
		return r, nil
	}
	if kind == "music" {
		for _, song := range record.Response.Data {
			u := song.AudioURL
			if u == "" {
				u = song.StreamAudioURL
			}
			if validMediaURL(u) {
				r.URLs = append(r.URLs, MediaURL{"audio", u})
			}
			if r.Title == "" {
				r.Title = song.Title
			}
			if r.CoverURL == "" && validMediaURL(song.ImageURL) {
				r.CoverURL = song.ImageURL
			}
		}
	}
	var encoded string
	if json.Unmarshal(record.ResultJSON, &encoded) == nil {
		record.ResultJSON = []byte(encoded)
	}
	var result struct {
		URLs []string `json:"resultUrls"`
	}
	_ = json.Unmarshal(record.ResultJSON, &result)
	urlKind := kind
	if kind == "music" {
		urlKind = "audio"
	}
	for _, u := range result.URLs {
		if validMediaURL(u) {
			r.URLs = append(r.URLs, MediaURL{urlKind, u})
		}
	}
	// A successful provider outcome remains billable even when delivery URLs
	// are malformed. Never infer a refund from missing downloadable content.
	return r, nil
}
func validMediaURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil
}

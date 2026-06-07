// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const githubLatestReleaseURL = "https://api.github.com/repos/uraniumdawn/karat/releases/latest"

type githubRelease struct {
	TagName string `json:"tag_name"`
}

// RunVersionChecker starts a background goroutine that checks for a newer release on GitHub
// and updates the status hint if one is available. Errors are silently ignored.
func (app *App) RunVersionChecker(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		latest, err := fetchLatestVersion(ctx)
		if err != nil {
			return
		}

		current := strings.TrimPrefix(Version, "v")
		latestClean := strings.TrimPrefix(latest, "v")

		if isNewerVersion(latestClean, current) {
			app.setVersionUpdate(latestClean)
		}
	}()
}

func fetchLatestVersion(ctx context.Context) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, githubLatestReleaseURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return "", err
	}

	return release.TagName, nil
}

// isNewerVersion reports whether b is strictly newer than a.
// Both must be dot-separated integer segments (e.g. "0.2.3").
func isNewerVersion(b, a string) bool {
	partsA := strings.SplitN(a, ".", 3)
	partsB := strings.SplitN(b, ".", 3)

	for i := range 3 {
		var va, vb int
		if i < len(partsA) {
			va, _ = strconv.Atoi(partsA[i])
		}
		if i < len(partsB) {
			vb, _ = strconv.Atoi(partsB[i])
		}
		if vb > va {
			return true
		}
		if vb < va {
			return false
		}
	}
	return false
}

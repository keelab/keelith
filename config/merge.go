package config

import (
	"fmt"
	"strconv"
	"strings"
)

// Merge combines Snapshots from low to high priority.
func Merge(snapshots ...Snapshot) (Snapshot, error) {
	if len(snapshots) == 0 {
		return Snapshot{}, fmt.Errorf("%w: no snapshots to merge", ErrInvalidSnapshot)
	}

	merged := make(map[string]any)
	revisions := make([]string, len(snapshots))
	for index, snapshot := range snapshots {
		if err := snapshot.validate(); err != nil {
			return Snapshot{}, fmt.Errorf("config: source %d: %w", index, err)
		}
		revisions[index] = snapshot.revision
		mergeMap(merged, snapshot.values)
	}

	values, err := cloneMap(merged, false)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		revision: combineRevisions(revisions),
		values:   values,
	}, nil
}

func mergeMap(target, overlay map[string]any) {
	for key, overlayValue := range overlay {
		if _, deleting := overlayValue.(deleteValue); deleting {
			delete(target, key)
			continue
		}

		overlayMap, overlayIsMap := overlayValue.(map[string]any)
		if overlayIsMap {
			currentMap, currentIsMap := target[key].(map[string]any)
			if !currentIsMap {
				currentMap = make(map[string]any)
			}
			mergeMap(currentMap, overlayMap)
			target[key] = currentMap
			continue
		}

		cloned, _ := cloneValue(overlayValue, true)
		target[key] = cloned
	}
}

func combineRevisions(revisions []string) string {
	if len(revisions) == 1 {
		return revisions[0]
	}
	var builder strings.Builder
	for index, revision := range revisions {
		if index > 0 {
			builder.WriteByte('|')
		}
		builder.WriteString(strconv.Itoa(len(revision)))
		builder.WriteByte(':')
		builder.WriteString(revision)
	}
	return builder.String()
}

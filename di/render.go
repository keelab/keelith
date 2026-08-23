package di

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// RenderJSON renders a stable indented graph manifest.
func RenderJSON(description Description) ([]byte, error) {
	return json.MarshalIndent(description, "", "  ")
}

// RenderText renders a human-readable provider list.
func RenderText(description Description) string {
	providers := append([]ProviderDescription(nil), description.Providers...)
	sort.Slice(providers, func(i, j int) bool { return providers[i].ID < providers[j].ID })
	var output bytes.Buffer
	fmt.Fprintf(&output, "root: %s\n", description.Root)
	for _, provider := range providers {
		fmt.Fprintf(&output, "- %s -> %s", provider.ID, provider.Type)
		if provider.Name != "" {
			fmt.Fprintf(&output, " [%s]", provider.Name)
		}
		if provider.Group != "" {
			fmt.Fprintf(&output, " [group=%s]", provider.Group)
		}
		fmt.Fprintf(&output, " scope=%s", provider.Scope)
		if provider.Decorator {
			output.WriteString(" decorator")
		}
		if provider.Override {
			output.WriteString(" override")
		}
		output.WriteByte('\n')
	}
	return output.String()
}

// RenderDOT renders a deterministic Graphviz dependency graph.
func RenderDOT(description Description) string {
	providers := append([]ProviderDescription(nil), description.Providers...)
	sort.Slice(providers, func(i, j int) bool { return providers[i].ID < providers[j].ID })
	var output bytes.Buffer
	output.WriteString("digraph keelith_di {\n  rankdir=LR;\n")
	rootID := "root"
	fmt.Fprintf(&output, "  %s [shape=box,label=%s];\n", rootID, strconv.Quote(description.Root))
	for index, provider := range providers {
		node := fmt.Sprintf("provider_%d", index)
		label := provider.Module + "\\n" + provider.Type
		fmt.Fprintf(&output, "  %s [label=%s];\n", node, strconv.Quote(label))
		if provider.Type == description.Root {
			fmt.Fprintf(&output, "  %s -> %s;\n", rootID, node)
		}
	}
	indexByID := make(map[string]int, len(providers))
	for index, provider := range providers {
		indexByID[provider.ID] = index
	}
	for _, edge := range description.Edges {
		from, fromExists := indexByID[edge.From]
		to, toExists := indexByID[edge.To]
		if fromExists && toExists {
			fmt.Fprintf(&output, "  provider_%d -> provider_%d [label=%s];\n", from, to, strconv.Quote(edge.Type))
		}
	}
	output.WriteString("}\n")
	return output.String()
}

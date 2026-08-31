package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/keelab/keelith/internal/scaffold"
)

// serviceAddJournal keeps the multi-file service workflow atomic from the
// caller's perspective. Individual scaffold operations already use atomic
// writes; this journal also restores earlier operations when a later registry
// synchronization rejects the project.
type serviceAddJournal struct {
	project   string
	snapshots []serviceAddSnapshot
}

type serviceAddSnapshot struct {
	relative string
	path     string
	exists   bool
	content  []byte
	mode     os.FileMode
}

func newServiceAddJournal(
	project string,
	options addOptions,
) (*serviceAddJournal, error) {
	packagePath := strings.ReplaceAll(options.packageID, ".", "/")
	serviceFile := serviceSnakeCase(options.service) + ".proto"
	implementationRelative := filepath.ToSlash(filepath.Join(
		"internal",
		"service",
		packagePath,
		serviceSnakeCase(options.service),
		"service.go",
	))
	relatives := []string{
		filepath.ToSlash(filepath.Join("api", packagePath, serviceFile)),
		filepath.ToSlash(filepath.Join(
			"api",
			packagePath,
			strings.TrimSuffix(serviceFile, ".proto")+".keelith.manifest.json",
		)),
		filepath.ToSlash(filepath.Join(
			"gen",
			packagePath,
			strings.TrimSuffix(serviceFile, ".proto")+".keelith.gen.go",
		)),
		implementationRelative,
		"internal/service/zz_keelith_register.gen.go",
		"internal/dependency/zz_keelith_clients.gen.go",
		"internal/component/zz_keelith_components.gen.go",
		"internal/consumer/zz_keelith_consumers.gen.go",
		"internal/job/zz_keelith_jobs.gen.go",
		"config/components/kustomization.yaml",
	}
	sort.Strings(relatives)
	unique := relatives[:0]
	for _, relative := range relatives {
		if len(unique) > 0 && unique[len(unique)-1] == relative {
			continue
		}
		unique = append(unique, relative)
	}

	journal := &serviceAddJournal{project: project}
	journal.snapshots = make([]serviceAddSnapshot, 0, len(unique))
	for _, relative := range unique {
		path, err := safeProjectFile(project, relative, true)
		if err != nil {
			return nil, err
		}
		info, err := os.Lstat(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			journal.snapshots = append(journal.snapshots, serviceAddSnapshot{
				relative: relative,
				path:     path,
			})
		case err != nil:
			return nil, fmt.Errorf("inspect service workflow file %s: %w", relative, err)
		case info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular():
			return nil, fmt.Errorf(
				"project file %q is not a regular file",
				relative,
			)
		default:
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil, fmt.Errorf("read service workflow file %s: %w", relative, readErr)
			}
			journal.snapshots = append(journal.snapshots, serviceAddSnapshot{
				relative: relative,
				path:     path,
				exists:   true,
				content:  append([]byte(nil), content...),
				mode:     info.Mode().Perm(),
			})
		}
	}
	return journal, nil
}

func serviceSnakeCase(value string) string {
	var output strings.Builder
	for index, r := range value {
		if unicode.IsUpper(r) {
			if index > 0 {
				output.WriteByte('_')
			}
			output.WriteRune(unicode.ToLower(r))
			continue
		}
		output.WriteRune(r)
	}
	return output.String()
}

func (journal *serviceAddJournal) rollback(cause error) error {
	if journal == nil {
		return cause
	}
	failures := []error{cause}
	for index := len(journal.snapshots) - 1; index >= 0; index-- {
		snapshot := journal.snapshots[index]
		if snapshot.exists {
			if err := atomicWriteFile(snapshot.path, snapshot.content, snapshot.mode); err != nil {
				failures = append(
					failures,
					fmt.Errorf("restore %s: %w", snapshot.relative, err),
				)
			}
			continue
		}
		if err := os.Remove(snapshot.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(
				failures,
				fmt.Errorf("remove %s: %w", snapshot.relative, err),
			)
		}
	}
	return errors.Join(failures...)
}

func executeAddService(
	ctx context.Context,
	project string,
	module string,
	options addOptions,
) (created, updated, unchanged []string, err error) {
	serviceName := strings.TrimSpace(options.service)
	shortName := strings.TrimSpace(options.name)
	if serviceName == "" {
		serviceName = exportedIdentifier(shortName) + "Service"
	}
	if options.packageID == "" {
		packageName := shortName
		if packageName == "" {
			packageName = strings.TrimSuffix(serviceName, "Service")
		}
		options.packageID = strings.ToLower(identifier(packageName)) + ".v1"
	}
	if options.method == "" {
		options.method = "Ping"
	}
	if options.httpMethod == "" {
		options.httpMethod = "GET"
	}
	if options.httpPath == "" {
		resource := strings.ToLower(identifier(shortName))
		if resource == "" {
			resource = strings.ToLower(identifier(strings.TrimSuffix(serviceName, "Service")))
		}
		options.httpPath = "/v1/" + resource + "/ping"
	}
	options.service = serviceName
	if err := requireServiceWiring(project); err != nil {
		return nil, nil, nil, err
	}

	journal, err := newServiceAddJournal(project, options)
	if err != nil {
		return nil, nil, nil, err
	}
	added, err := scaffold.AddService(ctx, scaffold.AddServiceOptions{
		Project:    project,
		Package:    options.packageID,
		Service:    serviceName,
		Method:     options.method,
		HTTPMethod: options.httpMethod,
		HTTPPath:   options.httpPath,
		Module:     module,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	created = append(created, added.Created...)
	updated = append(updated, added.Updated...)
	unchanged = append(unchanged, added.Unchanged...)

	synchronized, err := scaffold.SyncServices(ctx, project)
	if err != nil {
		return nil, nil, nil, journal.rollback(fmt.Errorf(
			"synchronize generated services: %w",
			err,
		))
	}
	created = append(created, synchronized.Created...)
	updated = append(updated, synchronized.Updated...)
	unchanged = append(unchanged, synchronized.Unchanged...)

	components, err := scaffold.SyncComponents(ctx, project, module)
	if err != nil {
		return nil, nil, nil, journal.rollback(fmt.Errorf(
			"synchronize generated components: %w",
			err,
		))
	}
	created = append(created, components.Created...)
	updated = append(updated, components.Updated...)
	unchanged = append(unchanged, components.Unchanged...)

	sort.Strings(created)
	sort.Strings(updated)
	sort.Strings(unchanged)
	return created, updated, unchanged, nil
}

func requireServiceWiring(project string) error {
	path := filepath.Join(project, builtinWiringProjectFile)
	info, err := os.Stat(path)
	if err == nil && info.Mode().IsRegular() {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect service wiring %s: %w", builtinWiringProjectFile, err)
	}
	return fmt.Errorf(
		"service additions require a generated wiring project; " +
			"use `keelith new NAME --template service` for a generated project",
	)
}

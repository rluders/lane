package lane

import (
	"go/parser"
	"go/token"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// fileImports returns the import paths of a Go source file.
func fileImports(t *testing.T, path string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		out = append(out, p)
	}
	return out
}

// The lifecycle and health model must not depend on HTTP or on built-in runners.
func TestArchitecture_CoreFilesIndependent(t *testing.T) {
	for _, file := range []string{"lane.go", "runner.go", "health.go", "signals.go"} {
		for _, imp := range fileImports(t, file) {
			if strings.HasPrefix(imp, "net/http") || strings.HasSuffix(imp, "/runners") {
				t.Errorf("%s imports %s", file, imp)
			}
		}
	}
}

// Built-in runners must not import the core package.
func TestArchitecture_RunnersDoNotImportCore(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "./runners").Output()
	if err != nil {
		t.Skipf("go list unavailable: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == "github.com/rluders/lane/v2" {
			t.Fatal("runners imports the lane core package")
		}
	}
}

package voiceintake

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kayushkin/llm-bridge/servicesettings"
)

func TestTheRegistryReadsDefaultsAndRefusesAnUndeclaredVariable(t *testing.T) {
	registry, err := NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{"LLMBRIDGE_SERVICE_TOKEN": "x"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := registry.String(SettingListenAddress); got != DefaultListenAddress {
		t.Errorf("listen address = %q, want %q", got, DefaultListenAddress)
	}
	if !strings.HasPrefix(DefaultListenAddress, "127.0.0.1:") {
		t.Errorf("the default listen address %q is not loopback", DefaultListenAddress)
	}
	_, err = NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{"VOICE_INTAKE_POOLSIZE": "1"}))
	if err == nil || !strings.Contains(err.Error(), "VOICE_INTAKE_POOLSIZE") {
		t.Fatalf("NewSettingsRegistry = %v, want a refusal naming the misspelled variable", err)
	}
}

func TestEveryEnvironmentVariableTheServiceReadsIsDeclared(t *testing.T) {
	declared := map[string]bool{}
	for _, definition := range SettingDefinitions() {
		declared[definition.EnvironmentVariable] = true
	}

	filesRead := 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		filesRead++
		ast.Inspect(file, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall || len(call.Args) == 0 {
				return true
			}
			selector, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}
			packageName, isIdentifier := selector.X.(*ast.Ident)
			if !isIdentifier || packageName.Name != "os" || (selector.Sel.Name != "Getenv" && selector.Sel.Name != "LookupEnv") {
				return true
			}
			literal, isLiteral := call.Args[0].(*ast.BasicLit)
			if !isLiteral {
				t.Errorf("%s reads an environment variable whose name is computed, which no declaration can be held to", path)
				return true
			}
			name, _ := strconv.Unquote(literal.Value)
			if !declared[name] {
				t.Errorf("%s reads %s, which no settings definition declares", path, name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join("cmd", "voice-intake", "main.go")); err != nil {
		t.Fatalf("the scan starts somewhere that is not the repository root: %v", err)
	}
	if filesRead < 4 {
		t.Fatalf("the scan read %d files; it is not looking at the service", filesRead)
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	logalexporter "github.com/CaliLuke/autok-logal/internal/exporter"
	logalstatus "github.com/CaliLuke/autok-logal/internal/status"
	logalstore "github.com/CaliLuke/autok-logal/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
)

var version = "dev"

func main() {
	if err := run(context.Background(), os.Args, os.Stdin, os.Stdout, os.Stderr); err != nil {
		jsonOutput := false
		for _, arg := range os.Args[1:] {
			if arg == "--json" || arg == "--json=true" {
				jsonOutput = true
			}
		}
		if jsonOutput {
			_ = json.NewEncoder(os.Stderr).Encode(map[string]any{"error": map[string]string{"code": "command_failed", "message": err.Error()}})
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) error {
	// Keep the Collector's native flags and subcommands compatible with runners.
	if len(args) == 1 {
		return runCollector(ctx, nil, out, errOut)
	}
	switch args[1] {
	case "serve":
		return runCollector(ctx, args[2:], out, errOut)
	case "services", "spans", "logs", "trace", "metrics", "sql", "reset-db", "help", "--help", "-h", "--version", "-v":
		return newCLI(in, out, errOut).Run(ctx, args)
	default:
		if strings.HasPrefix(args[1], "-") || args[1] == "components" || args[1] == "validate" || args[1] == "featuregate" || args[1] == "print-config" {
			return runCollector(ctx, args[1:], out, errOut)
		}
		return fmt.Errorf("unknown command %q; use logal --help", args[1])
	}
}

func runCollector(ctx context.Context, args []string, out, errOut io.Writer) error {
	settings := otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{Command: "logal", Description: "Local OpenTelemetry collector", Version: version},
		Factories: func() (otelcol.Factories, error) {
			return otelcol.Factories{
				Receivers:  map[component.Type]receiver.Factory{otlpreceiver.NewFactory().Type(): otlpreceiver.NewFactory()},
				Exporters:  map[component.Type]exporter.Factory{logalexporter.Type: logalexporter.NewFactory()},
				Extensions: map[component.Type]extension.Factory{logalstore.Type: logalstore.NewFactory(), logalstatus.Type: logalstatus.NewFactory()},
				Telemetry:  otelconftelemetry.NewFactory(),
			}, nil
		},
		ConfigProviderSettings: otelcol.ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{DefaultScheme: "file", ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory(), envprovider.NewFactory()}}},
	}
	command := otelcol.NewCommand(settings)
	command.SetArgs(args)
	command.SetOut(out)
	command.SetErr(errOut)
	command.SilenceErrors = true
	command.SilenceUsage = true
	return command.ExecuteContext(ctx)
}

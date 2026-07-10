package main

import (
	"fmt"
	"os"

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

func main() {
	settings := otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{Command: "logal", Description: "Auto-K local telemetry collector", Version: "2"},
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
	if err := command.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

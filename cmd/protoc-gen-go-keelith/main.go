// Command protoc-gen-go-keelith generates Keelith transport adapters.
package main

import (
	"github.com/keelab/keelith/internal/generator"
	"google.golang.org/protobuf/compiler/protogen"
)

func main() {
	settings := generator.Options{}
	protogen.Options{
		ParamFunc: settings.SetParameter,
	}.Run(func(plugin *protogen.Plugin) error {
		return generator.GeneratePluginWithOptions(plugin, settings)
	})
}

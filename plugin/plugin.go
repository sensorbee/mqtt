package plugin

import (
	"pfi/sensorbee/sensorbee/bql"
	"pfi/suma/mqtt"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("mqtt", bql.SourceCreatorFunc(mqtt.NewSource))
}

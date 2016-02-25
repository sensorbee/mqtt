package plugin

import (
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"pfi/suma/mqtt"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("mqtt", bql.SourceCreatorFunc(mqtt.NewSource))
	bql.MustRegisterGlobalSinkCreator("mqtt", bql.SinkCreatorFunc(mqtt.NewSink))
}

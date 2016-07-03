package plugin

import (
	"gopkg.in/sensorbee/mqtt.v1"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("mqtt", bql.SourceCreatorFunc(mqtt.NewSource))
	bql.MustRegisterGlobalSinkCreator("mqtt", bql.SinkCreatorFunc(mqtt.NewSink))
}

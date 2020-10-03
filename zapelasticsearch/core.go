package zapelasticsearch

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"

	elasticsearch6 "github.com/elastic/go-elasticsearch/v6"
	esapi6 "github.com/elastic/go-elasticsearch/v6/esapi"
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	esapi7 "github.com/elastic/go-elasticsearch/v7/esapi"
	elasticsearch8 "github.com/elastic/go-elasticsearch/v8"
	esapi8 "github.com/elastic/go-elasticsearch/v8/esapi"
	"go.elastic.co/ecszap"
)

//
// Core
//

type Core struct {
	zapcore.LevelEnabler
	encoder zapcore.Encoder
	fields  []zapcore.Field
	client6 *elasticsearch6.Client
	client7 *elasticsearch7.Client
	client8 *elasticsearch8.Client
	esIndex string
}

// NewCore6 new logger Core for elastic search v6
func NewCore6(enabler zapcore.LevelEnabler, encoderConfig zapcore.EncoderConfig, client *elasticsearch6.Client, esIndex string) *Core {
	newCore := &Core{
		LevelEnabler: enabler,
		client6:      client,
		esIndex:      esIndex,
	}
	initiateEncoder(newCore, encoderConfig)
	return newCore
}

// NewCore7 new logger Core for elastic search v7
func NewCore7(enabler zapcore.LevelEnabler, encoderConfig zapcore.EncoderConfig, client *elasticsearch7.Client, esIndex string) *Core {
	newCore := &Core{
		LevelEnabler: enabler,
		client7:      client,
		esIndex:      esIndex,
	}
	initiateEncoder(newCore, encoderConfig)
	return newCore
}

// NewCore8 new logger Core for elastic search v8
func NewCore8(enabler zapcore.LevelEnabler, encoderConfig zapcore.EncoderConfig, client *elasticsearch8.Client, esIndex string) *Core {
	newCore := &Core{
		LevelEnabler: enabler,
		client8:      client,
		esIndex:      esIndex,
	}
	initiateEncoder(newCore, encoderConfig)
	return newCore
}

func initiateEncoder(core *Core, encoderConfig zapcore.EncoderConfig) {
	ecsEncoderConfig := ecszap.ECSCompatibleEncoderConfig(encoderConfig)
	core.encoder = zapcore.NewJSONEncoder(ecsEncoderConfig)
}

func (core *Core) With(fields []zapcore.Field) zapcore.Core {
	// Clone core.
	clone := *core

	// Clone and append fields.
	clone.fields = make([]zapcore.Field, len(core.fields)+len(fields))
	copy(clone.fields, core.fields)
	copy(clone.fields[len(core.fields):], fields)

	// Done.
	return &clone
}

func (core *Core) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if core.Enabled(entry.Level) {
		return checked.AddCore(entry, core)
	}
	return checked
}

func (core *Core) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	// Generate the message.
	buffer, err := core.encoder.EncodeEntry(entry, fields)
	if err != nil {
		return errors.Wrap(err, "failed to encode log entry")
	}
	defer buffer.Free()

	if core.client6 != nil {
		req := esapi6.IndexRequest{
			Index:   core.esIndex,
			Body:    strings.NewReader(buffer.String()),
			Refresh: "true",
		}
		res, err := req.Do(context.Background(), core.client6)
		if err != nil {
			return err
		}
		defer res.Body.Close()

		if res.IsError() {
			return errors.Errorf("[%s] Error indexing document", res.Status())
		}
		return nil
	}

	if core.client7 != nil {
		req := esapi7.IndexRequest{
			Index:   core.esIndex,
			Body:    strings.NewReader(buffer.String()),
			Refresh: "true",
		}
		res, err := req.Do(context.Background(), core.client7)
		if err != nil {
			return err
		}
		defer res.Body.Close()

		if res.IsError() {
			return errors.Errorf("[%s] Error indexing document", res.Status())
		}
		return nil
	}

	if core.client8 != nil {
		req := esapi8.IndexRequest{
			Index:   core.esIndex,
			Body:    strings.NewReader(buffer.String()),
			Refresh: "true",
		}
		res, err := req.Do(context.Background(), core.client8)
		if err != nil {
			return err
		}
		defer res.Body.Close()

		if res.IsError() {
			return errors.Errorf("[%s] Error indexing document", res.Status())
		}
		return nil
	}

	return errors.Errorf("No client has initiated")
}

func (core *Core) Sync() error {
	return nil
}

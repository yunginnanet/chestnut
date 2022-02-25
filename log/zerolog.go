package log

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
)

type Zlog struct {
	l *zerolog.Logger
}

func (z Zlog) Debug(v ...interface{}) {
	z.l.Debug().Msgf(fmt.Sprintf("%+v", v))
}

func (z Zlog) Debugf(format string, v ...interface{}) {
	z.l.Debug().Msgf(format, v...)
}

func (z Zlog) Info(args ...interface{}) {
	z.l.Info().Msgf(fmt.Sprintf("%+v", args))
}

func (z Zlog) Infof(format string, v ...interface{}) {
	z.l.Info().Msgf(format, v...)
}

func (z Zlog) Warn(v ...interface{}) {
	var elog = *z.l
	for i, item := range v {
		switch item.(type) {
		case error:
			elog = z.l.With().Err(item.(error)).Logger()
			v = append(v[:i], v[i+1:]...)
		default:

		}
	}
	elog.Warn().Msgf(fmt.Sprintf("%+v", v...))
}

func (z Zlog) Warnf(format string, v ...interface{}) {
	z.l.Warn().Msgf(format, v...)
}

func (z Zlog) Error(v ...interface{}) {
	var elog = *z.l
	for i, item := range v {
		switch item.(type) {
		case error:
			elog = z.l.With().Err(item.(error)).Logger()
			v = append(v[:i], v[i+1:]...)
		default:

		}
	}
	elog.Error().Msgf(fmt.Sprintf("%+v", v))
}

func (z Zlog) Errorf(format string, v ...interface{}) {
	z.l.Error().Msgf(format, v...)
}

func (z Zlog) Panic(v ...interface{}) {
	z.l.Panic().Msg(fmt.Sprintf("%+v", v...))
}

func (z Zlog) Panicf(format string, v ...interface{}) {
	z.l.Panic().Msgf(format, v...)
}

func (z Zlog) Fatal(v ...interface{}) {
	z.l.Fatal().Msg(fmt.Sprintf("%+v", v...))
}

func (z Zlog) Fatalf(format string, v ...interface{}) {
	z.l.Fatal().Msgf(format, v...)
}

// Named adds a name string to the logger. How the name is added is
// logger specific i.e. a Zerolog field or std logger prefix, etc.
func Named(logger interface{}, name string) Logger {
	switch l := logger.(type) {
	case *zerolog.Logger:
		lg := l.With().Str("caller", name).Logger()
		return Zlog{l: &lg}
	case zerolog.Logger:
		llg := l.With().Str("caller", name).Logger()
		return Zlog{l: &llg}
	case Zlog:
		nzl := l.l.With().Str("caller", name).Logger()
		return Zlog{l: &nzl}
	}
	return nil
}

var levelTranslate = map[Level]zerolog.Level{
	DebugLevel: zerolog.TraceLevel,
	InfoLevel:  zerolog.InfoLevel,
	WarnLevel:  zerolog.WarnLevel,
	ErrorLevel: zerolog.ErrorLevel,
	PanicLevel: zerolog.PanicLevel,
	FatalLevel: zerolog.FatalLevel,
}

func NewZerologLoggerWithLevel(level Level) Zlog {
	lg := zerolog.New(os.Stdout)
	zerolog.SetGlobalLevel(levelTranslate[level])
	return Zlog{l: &lg}
}

var Log = NewZerologLoggerWithLevel(DebugLevel)

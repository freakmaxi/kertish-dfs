module github.com/freakmaxi/kertish-dfs/hooks-providers/rabbitmq

go 1.17

require (
	github.com/freakmaxi/kertish-dfs/basics v0.0.0-00010101000000-000000000000
	github.com/streadway/amqp v1.0.0
)

require (
	go.uber.org/atomic v1.6.0 // indirect
	go.uber.org/multierr v1.5.0 // indirect
	go.uber.org/zap v1.16.0 // indirect
)

replace github.com/freakmaxi/kertish-dfs/basics => ../../basics

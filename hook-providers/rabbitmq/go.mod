module github.com/freakmaxi/kertish-dfs/hooks-providers/rabbitmq

go 1.16

require (
	github.com/freakmaxi/kertish-dfs/basics v0.0.0-00010101000000-000000000000
	github.com/streadway/amqp v1.0.0
)

replace github.com/freakmaxi/kertish-dfs/basics => ../../basics

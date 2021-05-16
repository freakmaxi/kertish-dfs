module github.com/freakmaxi/kertish-dfs/data-node

go 1.16

require (
	github.com/freakmaxi/kertish-dfs/basics v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.2.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.16.0
)

replace github.com/freakmaxi/kertish-dfs/basics => ../basics

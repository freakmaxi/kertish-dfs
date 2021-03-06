module github.com/freakmaxi/kertish-dfs/head-node

go 1.16

require (
	github.com/freakmaxi/kertish-dfs/basics v0.0.0-00010101000000-000000000000
	github.com/freakmaxi/locking-center-client-go v0.2.1
	github.com/gorilla/mux v1.8.0
	go.mongodb.org/mongo-driver v1.5.2
	go.uber.org/zap v1.16.0
)

replace github.com/freakmaxi/kertish-dfs/basics => ../basics

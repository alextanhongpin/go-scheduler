include .env
export

include curl/Makefile

run:
	@go run -race cmd/main.go

run2:
	@PORT=8081 go run -race cmd/main.go

up:
	@docker-compose up -d

down:
	@docker-compose down

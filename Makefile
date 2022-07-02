include .env
export

include curl/Makefile

run:
	@go run -race cmd/main.go

up:
	@docker-compose up -d

down:
	@docker-compose down

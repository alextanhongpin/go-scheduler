include .env
export

run:
	@go run -race cmd/main.go

up:
	@docker-compose up -d

down:
	@docker-compose down

build:
	dep ensure
	go build -o client example/client.go

scan:
	snyk test
	snyk monitor

.SILENT:

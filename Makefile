all:
	go build tiflash_mock_autoscaler.go
fmt:
	go fmt .
image:
	docker build -t tiflash-mock-autoscaler -f ./Dockerfile .
pushimage:
	docker image tag tiflash-mock-autoscaler hub.pingcap.net/guojiangtao/tiflash-mock-autoscaler:latest
	docker image push hub.pingcap.net/guojiangtao/tiflash-mock-autoscaler:latest
baseimage:
	docker build -t base-tiflash-mock-autoscaler -f ./BaseDockerfile .
pushbaseimage:
	docker image tag base-tiflash-mock-autoscaler hub.pingcap.net/guojiangtao/base-tiflash-mock-autoscaler:latest
	docker image push hub.pingcap.net/guojiangtao/base-tiflash-mock-autoscaler:latest

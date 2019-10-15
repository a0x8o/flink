AUTHOR=banking
NAME=etherway
NETWORKID=15997
NETWORKPORT=30303
SUBNET=10.0.66
VERSION=latest
PWD=/dockerbackup
NETWORKNAME=etherway
RPCPORT=8545
ETHSTATSPORT=3000
FULLDOCKERNAME=$(AUTHOR)/$(NAME):$(VERSION)

build:
	docker build -t $(FULLDOCKERNAME) --build-arg WS_SECRET=$(PASSWD) .

start: network eth_one eth_two dashboard dashboardclient

stop:
	docker stop -t 0 eth_one
	docker stop -t 0 eth_two
	docker stop -t 0 dashboard
	docker stop -t 0 dashboardclient

clean:
	docker rm -f eth_one
	docker rm -f eth_two
	docker rm -f dashboard
	docker rm -f dashboardclient
	docker network rm $(NETWORKNAME)

cleanrestart: clean start

network:
	docker network create --subnet $(SUBNET).0/16 --gateway $(SUBNET).254 $(NETWORKNAME)

datavolumes:
	docker run -d -v ethereumeth_one:/root/.ethereum --name data-eth_eth_one --entrypoint /bin/echo $(AUTHOR)/$(NAME):$(VERSION)
	docker run -d -v ethereumeth_two:/root/.ethereum --name data-eth_eth_two --entrypoint /bin/echo $(AUTHOR)/$(NAME):$(VERSION)

rmnetwork:
	docker network rm $(NETWORKNAME)

help:
	docker run -i $(AUTHOR)/$(NAME):$(VERSION) help

eth_one:
	docker run -d --name=eth_one -h eth_one --net $(NETWORKNAME) --ip $(SUBNET).1 -e SUBNET=$(SUBNET) --volumes-from data-eth_eth_one -p $(NETWORKPORT):$(NETWORKPORT) -p $(RPCPORT):$(RPCPORT) $(AUTHOR)/$(NAME):$(VERSION) eth_one

eth_two:
	docker run -d --name=eth_two -h eth_two --net $(NETWORKNAME) --ip $(SUBNET).2 -e SUBNET=$(SUBNET) --volumes-from data-eth_eth_two -p $(RPCPORT) $(AUTHOR)/$(NAME):$(VERSION) eth_two

dashboardclient:
	docker run -d --name=dashboardclient -h dashboardclient --net $(NETWORKNAME) --ip $(SUBNET).3 -e SUBNET=$(SUBNET) $(AUTHOR)/$(NAME):$(VERSION) dashboardclient

dashboard:
	docker run -d --name=dashboard -h dashboard --net $(NETWORKNAME) --ip $(SUBNET).4 -e SUBNET=$(SUBNET) -p $(ETHSTATSPORT):$(ETHSTATSPORT) $(AUTHOR)/$(NAME):$(VERSION) dashboard

console:
	docker exec -ti eth_one /usr/local/sbin/geth attach ipc:/root/.ethereum/geth.ipc


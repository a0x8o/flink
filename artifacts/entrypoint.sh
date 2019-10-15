#!/bin/sh

OPTIONS="--nodiscover --networkid $NETWORKID --port $NETWORKPORT --rpc --rpcport $RPCPORT --etherbase adminetherbase --verbosity 6"
HELP="This is a help page. \
Available modes are: eth_one eth_two dashboard dashboardclient help."
case $1 in
	eth_one)
	cp /root/.ethereum/key.eth_one /root/.ethereum/geth/nodekey
	cp /root/.ethereum/static-nodes_two.json /root/.ethereum/geth/static-nodes.json
	sed -i "s/__subnet__/$SUBNET/g" /root/.ethereum/static-nodes.json
	/usr/local/sbin/geth --rpccorsdomain "*" --rpcapi admin,debug,shh,txpool,miner,personal,db,eth,net,web3 --identity $1 --rpcaddr $SUBNET.1 --mine --minerthreads "1" $OPTIONS
	;;
        eth_two)
	cp /root/.ethereum/key.eth_two /root/.ethereum/geth/nodekey
	cp /root/.ethereum/static-nodes_one.json /root/.ethereum/geth/static-nodes.json
	sed -i "s/__subnet__/$SUBNET/g" /root/.ethereum/static-nodes.json
	/usr/local/sbin/geth --rpccorsdomain "*" --rpcapi eth,net,web3,debug --identity $1 --rpcaddr $SUBNET.2 $OPTIONS
	;;
        dashboard)
	cd /eth-netstats ; npm start
	;;
        dashboardclient)
	sed -i "s/__subnet__/$SUBNET/g" /eth-net-intelligence-api/app.json
	cd /eth-net-intelligence-api ; pm2 start --no-daemon app.json
	;;
	help)
	echo $HELP
	;;
	"")
	echo $HELP
esac

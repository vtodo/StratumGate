from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.tcpclient import TCPClient

import argparse
import logging
import re

logging.basicConfig(format='%(asctime)s  %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)

class StratumProxy(TCPServer):
    "proxy module to avoid dev fee and others" 
    def __init__(self, pool, wallet, *args, **kwargs):
        self.pool = pool
        self.upstreams = {}
        self.wallet = wallet
        self.login_requests = {
            "total":0,
            "modified":0
        }
        super().__init__(*args, **kwargs)

    @gen.coroutine
    def setup_client(self, downstream):
        "setup upstream tcp client to the pool for each worker"
        def out(data):
            "pipe to downstream"
            logging.debug("<-- %s", data.decode())
            downstream.write(data)
            self.upstreams[downstream].read_until(b"\n", callback=out)
        upstream = yield TCPClient().connect(*self.pool.split(":"))
        upstream.set_nodelay(True)
        upstream.read_until(b"\n", callback=out)
        #use downstream as key for the newly opened connection
        self.upstreams.update({downstream:upstream})

    def close_upstream(self, downstream):
        "close upstream tcp client to the pool when worker disconnects from proxy"
        self.upstreams[downstream].close()
        del self.upstreams[downstream]                

    @gen.coroutine
    def handle_stream(self, stream, address):
        "handle proxy client connection. create upstream to pool per client"
        logging.info("client connected")
        stream.set_nodelay(True)        
        yield self.setup_client(stream)
        while True:
            try:
                data = yield stream.read_until(b"\n")
                data = data.decode()
                logging.debug("--> %s", data)
                login = re.compile('(?<="login":").+?(?=")')
                adr = re.findall(login, data)
                if adr and adr.count:# and 
                    self.login_requests["total"]+=1
                    if adr[0] != self.wallet:
                        self.login_requests["modified"]+=1
                        logging.info("request modified. wallet from: %s to %s", adr[0], self.wallet)
                        logging.info(   
                                        "%s requests modified - modified:%s, total:%s", 
                                        str(self.login_requests["modified"]/self.login_requests["total"]*100)+"%", 
                                        self.login_requests["modified"], 
                                        self.login_requests["total"]
                        )
                        # data = re.sub(login, self.wallet , data)
                    
                yield self.upstreams[stream].write(data.encode())

            except StreamClosedError:   
                logging.info("client disconnected")
                self.close_upstream(stream)
                break
            except (KeyboardInterrupt, SystemExit):
                logging.info("interrupted")                
                self.close_upstream(stream)
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stratum proxy')
    parser.add_argument('port', help="local port for the proxy")
    parser.add_argument('pool', help="pool address")
    parser.add_argument('wallet', help="wallet address to use")
    args = parser.parse_args()
    logging.info(   
                    "Running stratum proxy! port:%s redirecting to wallet:%s pool address:%s",
                    args.port, 
                    args.wallet, 
                    args.pool
                )
    try:        
        server = StratumProxy(pool=args.pool, wallet=args.wallet)
        server.bind(int(args.port))
        server.start()
        IOLoop.current().start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("interrupted")


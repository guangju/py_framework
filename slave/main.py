import sys
import os
import ConfigParser
import argparse
import traceback
sys.path.append('../')
from util.xml import xml_parse
from slave import server
from util.log import log

def main(argv=None):
    """
    main
    """

    log.init(debug=True)

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(vars(argv))
    program_name = os.path.basename(sys.argv[0])
    try:
        conf = xml_parse.XmlParse("../sconf/sconf.xml")
        slave_conf = conf.get_node(conf.root, "slave")
        server_node = conf.get_node(slave_conf[0], "server")
        server_val = conf.get_nodevalue(server_node[0])
        port_node = conf.get_node(slave_conf[0], "port")
        port_val = conf.get_nodevalue(port_node[0])
        '''
        if argv.port > 0:
            serverPort = argv.port
        else:
            serverPort = int(conftool.cf.get("server", "port"))

        if argv.cbport > 0:
            callbackPort = argv.cbport
        else:
            callbackPort = int(conftool.cf.get("cspub", "callback_port"))
        cspubutil.CALLBACK_PORT = callbackPort
        '''

        #slave = server.SlaveServer(port=callbackPort, handler=handle_spider_result, stat=stat)
        slave = server.SlaveServer(port = port_val)
        slave.start()




    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        traceback.print_exc()
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        raise(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--conf",
                        type=str,
                        default=None,
                        help="The path to a conf file"
                        )

    parser.add_argument("-p", "--port",
                        type=int,
                        default=0,
                        help="port"
                        )

    parser.add_argument("--cbport",
                        type=int,
                        default=0,
                        help="port"
                        )

    argv = parser.parse_args()
    exitcode = main(argv)
    sys.exit(exitcode)

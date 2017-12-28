from xml.dom import minidom
from util.log import log
import sys

class XmlParse(object):
    def __init__(self, conf_file):
        self.file = conf_file
        self.parse(self.file)

    def parse(self, file):
        try:
            self.dom = minidom.parse(file)
            self.root = self.dom.documentElement
           
        except Exception as e:
            log.fatal("xml parse error {}".format(e))
            sys.exit(1)

    def get_attrvalue(self, node, attrname):
         return node.getAttribute(attrname) if node else ''

    def get_nodevalue(self, node, index = 0):
        return node.childNodes[index].nodeValue if node else ''

    def get_value_by_name(self, node, name, index = 0):
        sub_node = node.getElementsByTagName(name) if node else []
        if sub_node:
            sub_node.childNodes[index].nodeValue if node else ''

    def get_node(self, node, name):
        return node.getElementsByTagName(name)

from plumbum import local
from plumbum import FG

def hadown(name):
    local.get('hadown')[name] & FG

def haup(name):
    local.get('haup')[name] & FG

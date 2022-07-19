"""An example magic"""
__version__ = '0.0.1'

import subprocess

from kernel import DyalogKernel
from ipykernel.kernelbase import Kernel

# TODO simply include aplmagic as a python module in setup.py
# TODO assignment / no printed result causes TypeError
# {∇⍵}⍬
# TODO closing kernel does not kill 'terp

from IPython.core.magic import (Magics, magics_class, line_cell_magic)

@magics_class
class APL(Magics):
    
    def __init__(self, shell):
        super(APL, self).__init__(shell)
        # self.data = data
        # Spawn Dyalog interpreter process and connect via RIDE
        self.dyalog = DyalogKernel()
    
    @line_cell_magic
    def apl(self, line, cell=None):
        if cell is None:
            [type, result] = self.dyalog.execute_block(line)
        else:
            [type, result] = self.dyalog.execute_block(cell)
        return print(result)

    def __del__(self):
        self.dyalog.do_shutdown(0)

def load_ipython_extension(ipython):
    ipython.register_magics(APL)

def unload_ipython_extension(ipython):
    pass

def hello():
    print("OK")
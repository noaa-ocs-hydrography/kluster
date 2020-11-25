import os
import numpy
normal_path = os.path.dirname(__file__)
install_path = os.path.join(os.path.dirname(numpy.__path__[0]), 'HSTB')
__path__ = [normal_path, install_path]

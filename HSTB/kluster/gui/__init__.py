import os
normal_path = os.path.dirname(__file__)
# os.path.normpath(os.path.join(normal_path, "../../Python2/HSTB"))
old_path = os.path.normpath(normal_path.replace("Python3", "Python2", 1))
__path__ = [normal_path, old_path]

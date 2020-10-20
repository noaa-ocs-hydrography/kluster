import os
normal_path = os.path.dirname(__file__)
# os.path.normpath(os.path.join(normal_path, "../../Python2/HSTB"))
# make this work in either the testing or final environment
old_path = os.path.normpath(normal_path.replace("Python38_test", "Python2", 1))
old_path = os.path.normpath(normal_path.replace("Python38", "Python2", 1))
__path__ = [normal_path, old_path]

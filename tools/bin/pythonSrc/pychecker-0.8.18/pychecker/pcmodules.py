"""
Track loaded PyCheckerModules together with the directory they were loaded from.
This allows us to differentiate between loaded modules with the same name
but from different paths, in a way that sys.modules doesn't do.
"""

__pcmodules = {}

def getPCModule(moduleName, moduleDir=None):
    global __pcmodules
    return __pcmodules.get((moduleName, moduleDir), None)

def getPCModules():
    global __pcmodules
    return __pcmodules.values()

def addPCModule(pcmodule):
    global __pcmodules
    __pcmodules[(pcmodule.moduleName, pcmodule.moduleDir)] = pcmodule

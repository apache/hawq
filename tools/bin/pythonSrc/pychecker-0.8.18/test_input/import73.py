class AbstractLib:
    ""
    def __init__(self):
        pass

    def abstract(self):
        "This is doc"
        "this is an expression, sneaky"
        raise NotImplementedError("This method must be overridden")

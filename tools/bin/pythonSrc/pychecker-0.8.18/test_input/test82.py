'test private globals in a class, from tom culliton'

_happy_private_name=("eyes", "nose", "fingers", "toes")
__unhappy_private_name=("missing life", "long commute", "dumb user")

def whatsit(thingee):
    if (thingee in _happy_private_name
    or  thingee in __unhappy_private_name):
        return "I got one too!"
    else:
        return "Ooo! What's that?"

class geek:
    'd'
    def __init__(self):
        self.happy = _happy_private_name
        self.unhappy = __unhappy_private_name


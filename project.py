from interface import *
from explain import *
from pprint import pprint


if __name__ == "__main__":
	# Initialise root window
	root = Tk()
	
	# Setting window attributes
	root.title("Query Comparer")
	root.attributes('-fullscreen', True)
	
	# Constructing GUI
	window = Window(root)
	
	root.mainloop()

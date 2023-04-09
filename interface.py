import tkinter as tk
from tkinter import *
from PIL import Image, ImageTk


class Window:
    def __init__(self, master):
        self.input_text = StringVar()
        self.input_text.set("temp")
        image1 = Image.open("test.png")
        test = ImageTk.PhotoImage(image1)

        # build entry frame
        self.frame_entry = Frame(master, background="red")
        self.label_input1 = Label(self.frame_entry, text="Query 1:")
        self.entry1 = Text(self.frame_entry, height=10, width=30)
        self.label_input2 = Label(self.frame_entry, text="Query 2:")
        self.entry2 = Text(self.frame_entry, height=10, width=30)
        
        # Build and pack button
        self.control_frame = Frame(master, background="yellow")
        self.button1 = Button(self.control_frame, text="Click to change text", command=self.get_entry1)
        self.button1.pack(expand=True, fill=BOTH)
        self.control_frame.pack(padx=10, pady=10, side=BOTTOM, expand=True, fill=BOTH)
        
        # Build and pack display text
        self.frame3 = Frame(master, background="green")
        self.textbox = Label(self.frame3, textvariable=self.input_text).pack(expand=True,fill=BOTH)
        self.frame3.pack(padx=10, pady=10, side=BOTTOM, expand=True, fill=BOTH)

        # packing entry frames
        self.label_input1.pack(expand=True, fill=BOTH)
        self.entry1.pack(fill=BOTH)
        self.label_input2.pack(expand=True, fill=BOTH)
        self.entry2.pack(fill=BOTH)
        self.frame_entry.pack(padx=10, pady=10, side=LEFT, expand=True, fill=BOTH)
        
        # Build and pack images
        self.frame_diagram = Frame(master, background="red")
        self.label_diagram1 = Label(self.frame_diagram, text="Query 1:").pack(expand=True, fill=BOTH)
        self.label1 = Label(self.frame_diagram, image=test)
        self.label1.image = test
        self.label1.pack(expand=True,fill=BOTH)
        self.label_diagram2 = Label(self.frame_diagram, text="Query 2:").pack(expand=True, fill=BOTH)
        self.label2 = Label(self.frame_diagram, image=test)
        self.label2.image = test
        self.label2.pack(expand=True,fill=BOTH)
        self.frame_diagram.pack(padx=10, pady=10, side=RIGHT, expand=True, fill=BOTH)

    def get_entry1(self):
        entry1 = self.entry1.get("1.0", 'end-1c')
        self.input_text.set(entry1)


# root = Tk()
# window = Window(root)
# root.mainloop()



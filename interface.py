import tkinter as tk
from tkinter import *
from PIL import Image, ImageTk


class Window:
    def __init__(self, master):
        self.input_text = StringVar()
        self.input_text.set("temp")
        image1 = Image.open("test.png")
        test = ImageTk.PhotoImage(image1)

        self.frame4 = Frame(master, background="yellow")
        self.button1 = Button(self.frame4, text="Click to change text", command=self.change_text)
        self.button1.pack(fill=BOTH)
        self.frame4.pack(padx=10, pady=10, side=BOTTOM, expand=True, fill=BOTH)

        self.frame3 = Frame(master, background="green")
        self.textbox = Label(self.frame3, textvariable=self.input_text).pack(fill=BOTH)
        self.frame3.pack(padx=10, pady=10, side=BOTTOM, expand=True, fill=BOTH)

        self.frame1 = Frame(master, background="red")
        self.label1 = Label(self.frame1, image=test)
        self.label1.image = test
        self.label1.pack()
        self.frame1.pack(padx = 10, pady=10, side=LEFT, expand=True, fill=BOTH)

        self.frame2 = Frame(master, background="blue")
        self.label2 = Label(self.frame2, image=test)
        self.label2.image = test
        self.label2.pack()
        self.frame2.pack(padx = 10, pady=10, side=LEFT, expand=True, fill=BOTH)

    def change_text(self):
        tempIN = input("hello: ")
        self.input_text.set(tempIN)


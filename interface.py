import tkinter as tk
from tkinter import *
from PIL import Image, ImageTk
from explain3 import *
from pprint import pprint


class Window:
    def __init__(self, master):
        self.input_text = StringVar()
        self.input_text.set("temp")
        image1 = Image.open("test.png")
        test = ImageTk.PhotoImage(Image.open("test.png"))

        # build entry frame
        self.frame_entry = Frame(master, background="red")
        self.label_input1 = Label(self.frame_entry, text="Query 1:")
        self.entry1 = Text(self.frame_entry, height=10, width=30)
        self.label_input2 = Label(self.frame_entry, text="Query 2:")
        self.entry2 = Text(self.frame_entry, height=10, width=30)
        
        # Build and pack button
        self.control_frame = Frame(master, background="yellow")
        self.button1 = Button(self.control_frame, text="Click to change text", command=self.run)
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

    def run(self):
        prefix = "set max_parallel_workers_per_gather =0;\n" \
                 "EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)\n"
        entry1 = prefix + self.entry1.get("1.0", 'end-1c')
        entry2 = prefix + self.entry2.get("1.0", 'end-1c')
        connection = connect_db()
        diag1 = graphviz.Digraph(graph_attr={'dpi': '50'})
        diag2 = graphviz.Digraph(graph_attr={'dpi': '50'})
        q1_result = execute_json(connection, entry1)
        q2_result = execute_json(connection, entry2)
        q1_root = buildQEP(q1_result)
        q2_root = buildQEP(q2_result)
        results = get_path_difference(q1_root, q2_root)
        print(results)
        QEP_bfs(q1_root, "query1", diag1)
        QEP_bfs(q2_root, "query2", diag2)
        img1 = ImageTk.PhotoImage(Image.open("query1.png"))
        img2 = ImageTk.PhotoImage(Image.open("query2.png"))
        self.label1.configure(image=img1)
        self.label1.image = img1
        self.label2.configure(image=img2)
        self.label2.image = img2
        self.input_text.set(results)


#root = Tk()
#window = Window(root)
#root.mainloop()



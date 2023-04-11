import tkinter as tk
from tkinter import *
from PIL import Image, ImageTk
from explain import *
from scrollable import ScrollableImage
from pprint import pprint

class Window:
    def __init__(self, master):
        test = ImageTk.PhotoImage(Image.open("test.png"))

        # build entry frame
        self.frame_entry = Frame(master, background="red")
        self.label_input1 = Label(self.frame_entry, text="Query 1:")
        self.entry1 = Text(self.frame_entry, height=10, width=30)
        self.label_input2 = Label(self.frame_entry, text="Query 2:")
        self.entry2 = Text(self.frame_entry, height=10, width=30)
        
        # Build and pack button
        self.control_frame = Frame(master, background="yellow")
        self.button1 = Button(self.control_frame, text="Submit", command=self.run)
        self.button1.pack(expand=True, fill=BOTH)
        self.control_frame.pack(padx=10, pady=10, side=BOTTOM, expand=True, fill=BOTH)
        
        # Build and pack display text
        self.frame3 = Frame(master, background="green", width= 1350, height= 150,)
        self.sql_label = Label(self.frame3, text="SQL Differences:").pack(side=LEFT, fill=BOTH)
        self.textbox_sql = Text(self.frame3, height=50, width=50, state="disabled")
        self.textbox_sql.pack(side=LEFT, expand=True, fill=X)
        self.qep_label = Label(self.frame3, text="    QEP Differences:").pack(side=LEFT, fill=BOTH)
        self.textbox_qep = Text(self.frame3, height=50, width=50, state="disabled")
        self.textbox_qep.pack(side=LEFT, expand=True, fill=X)
        self.frame3.pack(padx=10, pady=10, side=BOTTOM, expand=True)
        self.frame3.pack_propagate(False)

        # packing entry frames
        self.label_input1.pack(fill=BOTH)
        self.entry1.pack(expand=True, fill=BOTH)
        self.label_input2.pack(fill=BOTH)
        self.entry2.pack(expand=True, fill=BOTH)
        self.frame_entry.pack(padx=10, pady=10, side=LEFT, expand=True, fill=BOTH)
        
        # Build and pack images
        self.frame_diagram = Frame(master, background="red")
        self.query1_label = Label(self.frame_diagram, text="Query 1:").pack(fill=BOTH)
        self.image_scroll1 = ScrollableImage(self.frame_diagram, image=test, scrollbarwidth=20, width=400, height=300)
        self.image_scroll1.pack(expand=TRUE, fill=BOTH)
        self.query2_label = Label(self.frame_diagram, text="Query 2:").pack(fill=BOTH)
        self.image_scroll2 = ScrollableImage(self.frame_diagram, image=test, scrollbarwidth=20, width=400, height=300)
        self.image_scroll2.pack(expand=TRUE, fill=BOTH)
        self.frame_diagram.pack(padx=10, pady=10, side=RIGHT, expand=TRUE, fill=BOTH)

    def clear_frame(self, frame):
        for widgets in frame.winfo_children():
            widgets.destroy()

    def update_image_frame(self, frame):
        self.clear_frame(frame)
        query1 = ImageTk.PhotoImage(Image.open("query1.png"))
        query2 = ImageTk.PhotoImage(Image.open("query2.png"))
        self.query1_label = Label(self.frame_diagram, text="Query 1:").pack(expand=True, fill=BOTH)
        self.image_scroll1 = ScrollableImage(self.frame_diagram, image=query1, scrollbarwidth=20, width=400, height=300)
        self.image_scroll1.pack(expand=TRUE, fill=BOTH)
        self.query2_label = Label(self.frame_diagram, text="Query 2:").pack(expand=True, fill=BOTH)
        self.image_scroll2 = ScrollableImage(self.frame_diagram, image=query2, scrollbarwidth=20, width=400, height=300)
        self.image_scroll2.pack(expand=TRUE, fill=BOTH)

    def update_output(self, sql, qep):
        # unlock textboxes
        self.textbox_qep.config(state="normal")
        self.textbox_sql.config(state="normal")
        # clear textboxes
        self.textbox_qep.delete("1.0", END)
        self.textbox_sql.delete("1.0", END)
        # update textboxes
        self.textbox_qep.insert(END, qep)
        self.textbox_sql.insert(END, sql)
        # lock textboxes
        self.textbox_qep.config(state="disabled")
        self.textbox_sql.config(state="disabled")

    def run(self):
        prefix = "set max_parallel_workers_per_gather =0;\n" \
                 "EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)\n"
        entry1 = prefix + self.entry1.get("1.0", 'end-1c')
        entry2 = prefix + self.entry2.get("1.0", 'end-1c')
        entry1 = entry1.lower()
        entry2 = entry2.lower()
        # entry1 = q1
        # entry2 = q2
        connection = connect_db()
        diag1 = Diagram(name='', filename="query1", show=False)
        diag2 = Diagram(name='', filename="query2", show=False)
        q1_result = execute_json(connection, entry1)
        q2_result = execute_json(connection, entry2)
        q1_root = buildQEP(q1_result)
        q2_root = buildQEP(q2_result)
        results = get_path_difference(q1_root, q2_root)
        # print(results)
        QEP_dfs(q1_root, diag1)
        QEP_dfs(q2_root, diag2)
        self.update_image_frame(self.frame_diagram)
        self.update_output(results, results)


# root = Tk()
# window = Window(root)
# root.mainloop()



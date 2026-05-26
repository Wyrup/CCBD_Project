import matplotlib.pyplot as plt

class Visualisation:
  def __init__(self):
    # self.data = {}
    plt.ion() #activate the interactive mode
    self.fig, self.ax = plt.subplots(figsize=(10, 2))
    self.left = 0
    self.ax.set_yticks([])
    self.ax.set_xlabel("Time")

  def update(self, name: str, time: float):
    # self.data[name] = time
    # values = list(self.data.values())
    self.ax.barh(0, time, left=self.left, edgecolor="white", height=0.8, label=name)
    self.left += time
    
    self.ax.set_xlim(0, self.left * 1.05)
    self.ax.legend()
    plt.pause(0.25)

  def end(self):
    plt.ioff() # deactivate the interactive mode
    plt.show()
         
             
visu = Visualisation()

# myvisu = Visualisation()
# myvisu.update("generation", 20.5)
# myvisu.update("compaction", 3.5)
# myvisu.update("upload", 9.2)
# myvisu.update("listing", 2.6)
# myvisu.update("query", 3.6)
# myvisu.update("download", 12.8)
# myvisu.end()

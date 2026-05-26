import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(10, 2))
left = 0

ax.barh(0, 1, left=0, edgecolor="white", height=0.8, label="A")
ax.barh(0, 2, left=1, edgecolor="white", height=0.8, label="B")
ax.barh(0, 3, left=3, edgecolor="white", height=0.8, label="C")
# left += time
plt.legend()
plt.show()
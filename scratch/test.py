import math

# euclidean distance between points using UTM coordinates
p = [119771, 4277277]
q = [112942, 4275757]

# Calculate Euclidean distance
print (math.dist(p, q))


####

# eudlidean distance between points using WGS84 coordinates (should technically do haversine distance that
# accounts for earth's curvature, but probably doesn't matter much at this scale)
p = [-121.36299896, 38.56259918]
q = [-121.44100189, 38.54600143]

# Calculate Euclidean distance
print (math.dist(p, q))

####

# test plotting
import matplotlib.pyplot as plt
import numpy as np

xpoints = np.array([1, 8])
ypoints = np.array([3, 10])

plt.plot(xpoints, ypoints)
plt.show()


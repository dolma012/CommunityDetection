1. Overview of the Assignment
In this Assignment, you will implement the K-Means and Bradley-Fayyad-Reina (BFR) algorithm. The goal
is to help you be familiar with clustering algorithms with various distance measurements. The datasets
you are going to use are synthetic datasets.

2. Requirements
2.1 Programming Requirements
a. You must use Python to implement all tasks. Spark is not a requirement.

2.3 Programming Environment
Python 3.9.12

2.4 Write your own code
Do not share code with other students!!
For this assignment to be an effective learning experience, you must write your own code! We
emphasize this point because you will be able to find Python implementations of some of the required
functions on the web. Please do not look for or at any such code!
TAs will combine all the code we can find from the web (e.g., Github) as well as other students’ code
from this and other (previous) sections for plagiarism detection. We will report all detected plagiarism.

3. Datasets
Since the BFR algorithm has a strong assumption that the clusters are normally distributed with
independent dimensions, we have generated synthetic datasets by initializing some random centroids
and creating data points with these centroids and some standard deviations to form the clusters. We

have also added some data points as outliers. The “cluster” number of these outliers is represented by
-1 (i.e., no clusters). Figure 1 shows an example of the data points (in CSV format). The first column is the
data point index. The rest columns represent the features/dimensions of the data point.

You can access and download the following datasets from google drives.
a. Folder test1 (or test2) contains multiple files of data points. We will treat these files as separate data
chunks. In each iteration, you will load one file (one chunk of points) to the memory and process
these data points with the BFR algorithm.
b. Files cluster1.json and cluster2.json provide the ground truth cluster for the data points in test1 and
test2. The key is the data point index (as string). The value is its corresponding cluster index. The
cluster of outliers are represented as -1. Both datasets have 10 clusters (indexed as 0 to 9).
c. We have generated 10 testing sets using similar method (two of them are provided here, i.e., test1
and test2). Notice that the number of the dimensions, the number of the files, and the number of
the data points for each dataset could vary.

4. Tasks
You need to submit the following files on Gradescope: (all in lowercase)
a. [REQUIRED] Python scripts: bfr.py
b. [OPTIONAL] You can include other scripts to support your programs (e.g., callable functions).

You don’t need to include your results. TAs will grade the assignment using a separate testing dataset
(the data format remains the same).

4.1 Task description
You will write the K-Means and Bradley-Fayyad-Reina (BFR) algorithms from scratch. You should
implement K-Means as the main-memory clustering algorithm that you will use in BFR. You will
iteratively load the data points from a file and process these data points with the BFR algorithm. See
below pseudocode for your reference.

In BFR, there are three sets of points that you need to keep track of: Discard set (DS), Compression set
(CS), Retained set (RS). For each cluster in the DS and CS, the cluster is summarized by:

a. N: The number of points
b. SUM: the sum of the coordinates of the points
c. SUMSQ: the sum of squares of coordinates

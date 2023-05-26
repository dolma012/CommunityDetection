CSCI 5523: Introduction to Data Mining


1. Overview of the Assignment
In this assignment, you will explore the Spark GraphFrames library as well as implement your own
Girvan-Newman algorithm using the Spark Framework to detect communities in graphs. You will use the
ub_sample_data.csv dataset to find users who have a similar business taste. The goal of this assignment
is to help you understand how to use the Girvan-Newman algorithm to detect communities in an
efficient way within a distributed environment.

2. Requirements
2.1 Programming Requirements
a. You must use Python and Spark to implement all tasks.
b. You can use the Spark DataFrame and GraphFrames library for task1, but for task2 you can ONLY use
Spark RDD and standard Python libraries.
2.2 Submission Platform
We will use Gradescope to automatically run and grade your submission. You must test your scripts on
the local machine before submission.

2.3 Programming Environment
Python 3.9.12 and Spark 3.2.1

2.4 Write your own code
Do not share code with other students!!
For this assignment to be an effective learning experience, you must write your own code! We
emphasize this point because you will be able to find Python implementations of some of the required
functions on the web. Please do not look for or at any such code!
TAs will combine all the code we can find from the web (e.g., Github) as well as other students’ code
from this and other (previous) sections for plagiarism detection. We will report all detected plagiarism.

3. Datasets
You will continue to use the Yelp dataset. We have generated a sub-dataset, ub_sample_data.csv, from
the Yelp review dataset containing user_id and business_id. You can access and download the data on
Google Drive. TAs will use a different test dataset for grading.

4. Tasks
You need to submit the following files on Gradescope: (all in lowercase)
a. Python scripts: task1.py, task2.py
b. [OPTIONAL] You can include other scripts to support your programs (e.g., callable functions).

You don’t need to include your results. TAs will grade the assignment using a separate testing dataset
(the data format remains the same).

4.1 Graph Construction
To construct a social network graph, each node represents a user and there will be an edge between two
nodes if the number of times that two users review the same business is greater than or equivalent to
the filter threshold. For example, suppose user1 reviewed [business1, business2, business3] and user2
reviewed [business2, business3, business4, business5]. If the threshold is 2, there will be an edge
between user1 and user2.
If the user node has no edge, we will not include that node in the graph.
In this assignment, we use the filter threshold 7.

4.2 Task1: Community Detection Based on GraphFrames (2 pts)
4.2.1 Task description
In task1, you will explore the Spark GraphFrames library to detect communities in the network graph you
constructed in 4.1. In the library, it provides the implementation of the Label Propagation Algorithm
(LPA) which was proposed by Raghavan, Albert, and Kumara in 2007. It is an iterative community
detection solution whereby information “flows” through the graph based on underlying edge structure.
In this task, you do not need to implement the algorithm from scratch, you can call the method for LPA
provided by the library. The parameter “maxIter” for LPA should be set to 5. The following websites may
help you get started with the Spark GraphFrames:
https://docs.databricks.com/spark/latest/graph-analysis/graphframes/user-guide-python.html
https://graphframes.github.io/graphframes/docs/_site/user-guide.html
4.2.2 Input Format (Please make sure you use exactly the same input parameters names!)
We use the “argparse” module to parse the following arguments.

Parameters in task1.py
1. Filter threshold(--filter_threshold): the filter threshold to generate edges between user
nodes.
2. Input file Path(--input_file): the path to the input file including path, file name and extension.
3. community output file path (--community_output_file): the path to the community output
file including path, file name and extension.

Execution example:
$ spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 task1.py
--filter_threshold <filter_threshold> --input_file <input_file> --community_output_file
<community_output_file>
Example: spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 task1.py
--filter_threshold 7 --input_file data.csv --community_output_file out1

4.2.3 Output Result
In this task, you need to save your result of communities in a text file (.txt). Each line represents one
community and the format is:

‘user_id1’, ‘user_id2’, ‘user_id3’, ‘user_id4’, ...

Your result should be firstly sorted by the size of communities in the ascending order and then the first
user_id in the community in lexicographical order (the user_id is type of string). The user_ids in each
community should also be in the lexicographical order.
If there is only one node in the community, we still regard it as a valid community.

Figure 1: community output file format

4.3 Task2: Community Detection Based on Girvan-Newman algorithm (8 pts)

In task2, you will implement your own Girvan-Newman algorithm to detect the communities in the
network graph. Because the task1 and task2 code will be executed separately, you need to construct the
graph again in this task following the rules in section 4.1. You can refer to the Chapter 10 from the
Mining of Massive Datasets book for the algorithm details.
For task2, you can ONLY use Spark RDD and standard Python libraries. Remember to delete your code
that imports graphframes.
4.3.1 Betweenness Calculation (4 pts)
In this part, you will calculate the betweenness of each edge in the original graph you constructed in 4.1.
Then you need to save your result in a .txt file. The format of each line is
(‘user_id1’, ‘user_id2’), betweenness value

Your result should be firstly sorted by the betweenness values in the descending order and then the first
user_id in the tuple in lexicographical order (the user_id is type of string). The two user_ids in each tuple
should also be in lexicographical order. You do not need to round your result.

Figure 2: betweenness output file format

4.3.2 Community Detection (4 pts)
You are required to divide the graph into suitable communities, which reaches the global highest
modularity. The formula of modularity is shown below:

According to the Girvan-Newman algorithm, after removing one edge, you should re-compute the
betweenness. The “m” in the formula represents the edge number of the original graph. The “A” in the
formula is the adjacent matrix of the original graph. (Hint: In each remove step, “m” and “A” should not
be changed).
If the community only has one user node, we still regard it as a valid community.
You need to save your result in a .txt file. The format is the same with the output file from task1.

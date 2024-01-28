

# Overview: Explored the Spark GraphFrames library as well as implemented Girvan-Newman algorithm using the Spark Framework/distributed environment to detect communities in graphs. Used the ub_sample_data.csv dataset to find users who have a similar business taste.


## Requirements: 
a. Python and Spark to implement all tasks.
b. Spark DataFrame and GraphFrames library for task1
c. ONLY Spark RDD and standard Python libraries for task2

# 2.3 Programming Environment
Python 3.9.12 and Spark 3.2.1

# 4.1 Graph Construction
To construct a social network graph, each node represents a user and there will be an edge between two
nodes if the number of times that two users review the same business is greater than or equivalent to
the filter threshold. For example, suppose user1 reviewed [business1, business2, business3] and user2
reviewed [business2, business3, business4, business5]. If the threshold is 2, there will be an edge
between user1 and user2.
If the user node has no edge, we will not include that node in the graph.
In this assignment, we use the filter threshold 7.

## 4.2 Task1: Community Detection Based on GraphFrames (2 pts)

# Parameters in task1.py
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

# 4.3 Task2: Community Detection Based on Girvan-Newman algorithm (8 pts)

In task2, implemented Girvan-Newman algorithm to detect the communities in the
network graph. 

4.3.1 Betweenness Calculation (4 pts)
Calculated the betweenness of each edge in the original graph you constructed in 4.1. Result saved in a .txt file. The format of each line is
(‘user_id1’, ‘user_id2’), betweenness value

Result is firstly sorted by the betweenness values in the descending order and then the first
user_id in the tuple in lexicographical order (the user_id is type of string). The two user_ids in each tuple
should also be in lexicographical order. 

Figure 2: betweenness output file format

4.3.2 Community Detection (4 pts)
Divided the graph into suitable communities, which reaches the global highest
modularity. The formula of modularity is shown below:

1. Overview of the Assignment
In this assignment, you will implement the SON algorithm using the Apache Spark Framework. You will
develop a program to find frequent itemsets in two datasets, one simulated dataset and one real-world
dataset generated from Yelp dataset. The goal of this assignment is to apply the algorithms you have
learned in class on large datasets more efficiently in a distributed environment.

2. Requirements
2.1 Programming Requirements
a. You must use Python to implement all tasks. You can only use standard python libraries (i.e., external
libraries like numpy or pandas are not allowed).
b. You are required to only use Spark RDD, i.e. no point if using Spark DataFrame or DataSet.
2.2 Submission Platform
We will use a submission platform to automatically run and grade your submission. We highly
recommend that you first test your scripts on your local machine before submitting your solutions. We
will use Gradescope to grade your submissions. The Gradescope submission site will be available on
02/20/2023.
2.3 Programming Environment
Python 3.9.12, and Spark 3.2.1
2.4 Write your own code
Do not share code with other students!!
For this assignment to be an effective learning experience, you must write your own code! We
emphasize this point because you will be able to find Python implementations of some of the required
functions on the web. Please do not look for or at any such code!
TAs will combine all the code we can find from the web (e.g., Github) as well as other students’ code
from this and other (previous) sections for plagiarism detection. We will report all detected plagiarism to
the university.

3. Datasets
In this assignment, you will implement the algorithms using simulated and real-world datasets. In Task 1,
you will build and test your program with two small simulated CSV files (small1.csv and small2.csv).
In Task 2, you need to first generate a subset using business.json and review.json from the Yelp dataset
(see Section 4.2.1). The dataset should have the same structure as the simulated datasets. You will test
your program with this real-world dataset locally (you do not need to submit the dataset). TAs will use
different sampled subsets of the Yelp datasets for grading.
Figure 1 shows the file structure, the first column is “user_id” and the second column is “business_id”.

Figure 1: Input Data Format

4. Tasks
In this assignment, you will implement the SON algorithm to solve all tasks (Task 1 and Task 2) on top of
Apache Spark Framework. You need to find all the possible combinations of the frequent itemsets in
any given input file within the required time. You can refer to Chapter 6 from the Mining of Massive
Datasets book and concentrate on section 6.4 – Limited-Pass Algorithms. (Hint: you can choose either
A-Priori, MultiHash, or PCY algorithm to process each chunk of the data)
4.0 Submission
You need to submit the following files on Gradescope:
a. (Required) Two Python scripts (all lowercase): task1.py, task2.py
b. (Optional) Other Python scripts containing functions
4.1 Task 1: Simulated data (6 pts)
There are two CSV files (small1.csv and small2.csv) provided in the data folder. The small1.csv is a sample
file that you can use to debug your code. TAs will grade your code with small2.csv.
4.1.1 Task Description
You need to create two kinds of market-basket models and build the algorithm for the two cases. More
specifically, you will calculate the combinations of frequent businesses (Case 1) and frequent users
(Case 2), including singletons, pairs, triples, etc., that are qualified as frequent given a support threshold.
Case 1 (3 pts):
You will create a basket for each user containing the business ids reviewed by this user. If a business was
reviewed more than once by a reviewer, we consider this business was rated only once, i.e., the business
ids within each basket are unique. The generated baskets will be like:

user1: [business11, business12, business13, ...]
user2: [business21, business22, business23, ...]
user3: [business31, business32, business33, ...]

Case 2 (3 pts):
You will create a basket for each business containing the user ids who reviewed this business. Similar to
case 1, the user ids within each basket are unique. The generated will be like:
business1: [user11, user12, user13, ...]
business2: [user21, user22, user23, ...]
business3: [user31, user32, user33, ...]

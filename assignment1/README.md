3. Yelp Data
In this assignment, you are provided with two datasets (reviews.json and business.json) extracted from the Yelp dataset for developing hw1.
4. Tasks
4.1 Task1: Data Exploration 

4.1.1 Task description
You will explore the review dataset and write a program to answer the following questions: A. The total number of reviews
B. The number of reviews in a given year, y
C. The number of distinct users who have written the reviews
D. Top m users who have the largest number of reviews and its count
E. Top n frequent words in the review text. The words should be in lower cases. The following punctuations i.e., “(”, “[”, “,”, “.”, “!”, “?”, “:”, “;”, “]”, “)”, and the given stopwords are excluded. You can access the file that contains stopwords from the “data” folder on Google Drive.

4.1.2 Execution commands
You need to use the following piece of code for reading the parameters:
You can execute task1.py using the following command line:
$ python task1.py -- input_file <input_file> --output_file <output_file> --stopwords <stopwords> --y <y>
--m <m> --n <n> Params:
input_file – the input file (the review dataset)
output_file – the output file contains your answers
stopwords – the file contains the stopwords that should be removed for Question E
y/m/n – see 4.1.1 4.1.3 Output format:

You must write the results in the JSON format using exactly the same tags for each question (see an example in Figure 2). The answer for A/B/C is a number. The answer for D is a list of pairs [user, count]. The answer for E is a list of frequent words. All answers should be sorted by the count in the descending order. If two users/words have the same count, please sort them in the alphabetical order.

4.2 Task2: Exploration on Multiple Datasets

4.2.1 Task description
In task2, you will explore the two datasets together (review and business datasets) and write a program to compute the average stars for each business category (i.e., first joining datasets then taking average) and output top n categories with the highest average stars. The business categories should be extracted from the “categories” tag in the business file and split by comma (also need to remove leading and trailing spaces for the extracted categories).

4.2.2 Execution commands (using argparse as 4.1.2)
Python: $ python task2.py --review_file <review_file> --business_file <business_file> --output_file <output_file> --n <n>
Params:
review_file – the input file (the review dataset) business_file – the input file (the business dataset) output_file – the output file contains your answers
n – top n categories with highest average stars (see 4.2.1)
4.2.3 Output format:
You must write the results in the JSON format using exactly the same tags (see an example in Figure 3). The answer is a list of pairs [category, stars], which are sorted by the stars in the descending order. If two categories have the same value, please sort the categories in the alphabetical order.

4.3 Task3: Partition
4.3.1 Task description
In this task, you will learn how partitions work in the RDD. You need to compute the businesses that have more than n reviews in the review file. At the same time, you need to show the number of partitions for the RDD and the number of items per partition. You will implement the function using (1) the default
  
partition function with the default partition number; (2) a customized partition function with an input n_partitions, i.e.. design a customized partition function (like a hash function). You need to describe the customized partition function you use, compare the execution time of the two methods (default vs. customized), and examine how the number of partitions affect the execution time. You need to justify the result with one or two sentences (write your answer in a PDF file “task3.pdf”).
4.3.2 Execution commands
$ python task3_default.py --input_file <input_file> --output_file <output_file> --n <n> Params:
input_file – the input file (the review dataset) output_file – the output file contains your answers
n – the threshold of the number of reviews (see 4.3.1)
$ python task3_customized.py --input_file <input_file> --output_file <output_file> --n_partitions <n_partitions> --n <n>
Params:
input_file – the input file (the review dataset) output_file – the output file contains your answers n_partitions – the number of partitions (e.g., 10)
n – the threshold of the number of reviews (see 4.3.1)
4.3.3 Output format:
You must write the results in the JSON format using exactly the same tags (see an example in Figure 4). The answer for the number of partitions is a number. The answer for the number of items per partition is a list of numbers. The answer for the result is a list of pairs [business, count] (no need to sort).

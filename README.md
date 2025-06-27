# luigi-demo

This repository contains a Luigi-based data pipeline demo for fetching, processing, and analyzing text data from Project Gutenberg. The pipeline downloads top books, cleans their text, counts word frequencies, and generates a summary of the most frequent words across all downloaded books.

# Project Structure

data/: This directory is used to store intermediate and final outputs of the Luigi pipeline.

book_list.txt: Stores the URLs of the top downloaded books.

counts/: Contains individual pickle files for word counts of each downloaded book.

summary.txt: Contains the final summary of the most frequent words across all books.

Luigi_Demo.ipynb: A Jupyter Notebook demonstrating the Luigi pipeline setup and execution.

luigi-demo.py: The Python script containing the Luigi tasks.

README.md: This README file.

# Setup and Installation
To set up and run this project, follow these steps:

Initialize Python Virtual Environment and Install Dependencies:
Open your terminal and run the following commands to create a virtual environment and install the necessary Python packages:

Bash

python3 -m venv luigi-venv
source luigi-venv/bin/activate
pip install wheel luigi beautifulsoup4 requests
Start the Luigi Scheduler:
Luigi requires a central scheduler to manage the tasks. Open a new terminal window and start the scheduler:

Bash

luigid --port 8082
This will start the Luigi scheduler on http://localhost:8082. You can access the UI in your web browser to monitor the pipeline's progress.

# Running the Pipeline
You can run the Luigi pipeline from the command line or using the provided Jupyter Notebook.

Running from the Command Line
To execute the pipeline and generate the word frequency summary, navigate to the project root directory in your terminal (where luigi-demo.py is located) and run:

Bash

python -m luigi --module luigi-demo TopWords --GlobalParams-NumberBooks 10 --GlobalParams-NumberTopWords 500
--module luigi-demo: Specifies the Python module containing the Luigi tasks.

TopWords: This is the target task for the pipeline. Luigi will automatically determine and run all its dependencies.

--GlobalParams-NumberBooks 10: Sets the NumberBooks parameter in GlobalParams to 10, meaning the pipeline will process the top 10 books.

--GlobalParams-NumberTopWords 500: Sets the NumberTopWords parameter in GlobalParams to 500, meaning the final summary will list the top 500 most frequent words.

# Running with Jupyter Notebook
The Luigi_Demo.ipynb notebook provides a step-by-step guide to running the pipeline. You can open and run the cells in the notebook after setting up the virtual environment and starting the Luigi scheduler.

# Pipeline Description
The pipeline consists of the following Luigi tasks:

GetTopBooks
Purpose: Retrieves the URLs of the top 100 EBooks from Project Gutenberg's "Top 100 EBooks yesterday" list.

Output: data/book_list.txt - A text file containing one URL per line.

DownloadBooks
Purpose: Downloads a specific book identified by FileID from the URLs obtained by GetTopBooks, removes special characters, and converts the text to lowercase.

Requires: GetTopBooks

Parameters: FileID (integer, default=0) - The index of the book URL to download from book_list.txt.

Output: data/downloads/{FileID}.txt - A text file containing the cleaned content of the downloaded book.

CountWords
Purpose: Calculates the word frequency for a given downloaded book.

Requires: DownloadBooks (for a specific FileID)

Parameters: FileID (integer, default=0) - The index of the book for which to count words.

Output: data/counts/count_{FileID}.pickle - A pickle file storing a collections.Counter object containing word frequencies for the book.

# GlobalParams
Purpose: A Luigi Config class to define global parameters for the pipeline.

Parameters:

NumberBooks (integer, default=10): The number of top books to process.

NumberTopWords (integer, default=500): The number of most frequent words to report in the final summary.

TopWords
Purpose: Aggregates word counts from all processed books and generates a combined summary of the most frequent words.

Requires: Multiple CountWords tasks, one for each book specified by GlobalParams().NumberBooks.

Output: data/summary.txt - A text file listing the most frequent words and their combined counts.

# Monitoring the Pipeline
Once the Luigi scheduler is running (luigid --port 8082), you can open your web browser and navigate to http://localhost:8082 to view the Luigi UI. This interface provides a visual representation of your pipeline, showing task dependencies, status (pending, running, done, failed), and logs.

# Import necessary libraries
import requests
import luigi
from bs4 import BeautifulSoup as bs
from collections import Counter
import pickle
import io
import os

# Define a Luigi Task to get the URLs of the top downloaded books from Project Gutenberg.
class GetTopBooks(luigi.Task):
    """
    Task to fetch the URLs of the top 100 eBooks from Project Gutenberg.
    """

    def output(self):
        """
        Specifies the local file to store the URLs of the top downloaded books.
        """
        return luigi.LocalTarget('data/book_list.txt')

    def run(self):
        """
        Retrieves URLs of top downloaded books from the Project Gutenberg website
        and stores them in the local file.
        """
        resp = requests.get('https://www.gutenberg.org/browse/scores/top')
        soup = bs(resp.content, 'html.parser')

        header = soup.find_all("h2", string="Top 100 EBooks yesterday")[0]
        top_list = header.find_next_sibling("ol")

        with io.open(self.output().path, 'w', encoding='utf-8') as f:
            for result in top_list.select('li>a'):
                if '/ebooks/' in result['href']:
                    f.write('http://www.gutenberg.org{link}.txt.utf-8\n'.format(link=result['href']))

# Define a Luigi Task to download individual books.
class DownloadBooks(luigi.Task):
    """
    Task to download a specific book, clean its text, and store it locally.
    """

    FileID = luigi.IntParameter(default=0)
    replace_list = """.,"';_[]:*-"""

    def requires(self):
        """
        Declares that this task requires the GetTopBooks task to have run.
        """
        return GetTopBooks()

    def output(self):
        """
        Specifies the local file where the downloaded and cleaned book will be stored.
        """
        output_dir = 'data/downloads'
        os.makedirs(output_dir, exist_ok=True)
        return luigi.LocalTarget(f'{output_dir}/{self.FileID}.txt')

    def run(self):
        """
        Downloads a specific book, removes special characters, converts to lowercase,
        and saves the cleaned text.
        """
        with self.input().open('r') as i:
            url = i.read().splitlines()[self.FileID]

            with io.open(self.output().path, 'w', encoding='utf-8') as f:
                book = requests.get(url, timeout=60)
                book_text = book.text

                for char in self.replace_list:
                    book_text = book_text.replace(char, ' ')

                book_text = book_text.lower()
                f.write(book_text)

# Define a Luigi Task to count words in a downloaded book.
class CountWords(luigi.Task):
    """
    Task to count word frequencies for a single downloaded book.
    """
    FileID = luigi.IntParameter(default=0)

    def requires(self):
        """
        Declares that this task requires the DownloadBooks task for the corresponding FileID.
        """
        return DownloadBooks(FileID=self.FileID)

    def output(self):
        """
        Specifies the local pickle file to store the word counts.
        """
        output_dir = 'data/counts'
        os.makedirs(output_dir, exist_ok=True)
        file_path = f'{output_dir}/count_{self.FileID}.pickle'
        return luigi.LocalTarget(file_path, format=luigi.format.Nop)

    def run(self):
        """
        Generates word frequencies for the downloaded book and stores as a binary.
        """
        input_path = self.input().path
        with io.open(input_path, 'r', encoding='utf-8') as i:
            word_count = Counter(i.read().split())
            with io.open(self.output().path, 'wb') as f:
                pickle.dump(word_count, f)

# Set global default parameters for number of books to download and number of most frequent words to report
class GlobalParams(luigi.Config):
    """
    Global configuration parameters for the Luigi pipeline.
    """
    NumberBooks = luigi.IntParameter(default=10)
    NumberTopWords = luigi.IntParameter(default=500)

# Define the final Luigi Task to aggregate word counts and report top words.
class TopWords(luigi.Task):
    """
    Task to aggregate word counts from all processed books and generate a
    combined summary of the most frequent words.
    """

    def requires(self):
        """
        Declares that this task requires multiple instances of the CountWords task.
        """
        inputs = []
        for i in range(GlobalParams().NumberBooks):
            inputs.append(CountWords(FileID=i))
        return inputs

    def output(self):
        """
        Specifies the local file where the combined word frequency summary will be stored.
        """
        return luigi.LocalTarget('data/summary.txt')

    def run(self):
        """
        Retrieves word count summaries from each dependent task,
        merges them into a combined total, and writes the most frequent words.
        """
        total_count = Counter()

        for input in self.input():
            with input.open('rb') as i:
                nextCounter = pickle.load(i)
                total_count += nextCounter

        with io.open(self.output().path, 'w', encoding='utf-8') as f:
            for item in total_count.most_common(GlobalParams().NumberTopWords):
                f.write('{0: <15}{1}\n'.format(*item))

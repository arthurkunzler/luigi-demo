import requests
import luigi
from bs4 import BeautifulSoup as bs
from collections import Counter
import pickle
import io
import os

class GetTopBooks(luigi.Task):
    # Specify local file to store urls of top downloaded books
    def output(self):
        return luigi.LocalTarget('data/book_list.txt')

    # Retreive urls of top downloaded books from website and storing in the local file
    def run(self):
        resp = requests.get('https://www.gutenberg.org/browse/scores/top')
        soup = bs(resp.content, 'html.parser')

        header = soup.find_all("h2", string="Top 100 EBooks yesterday")[0]
        top_list = header.find_next_sibling("ol")

        with io.open(self.output().path, 'w', encoding='utf-8') as f:
            for result in top_list.select('li>a'):
                if '/ebooks/' in result['href']:
                    f.write('http://www.gutenberg.org{link}.txt.utf-8\n'.format(link=result['href']))

class DownloadBooks(luigi.Task):

    FileID = luigi.IntParameter(default=0)
    replace_list = """.,"';_[]:*-"""

    # Get previously stored urls as input
    def requires(self):
        return GetTopBooks()

    # Specify local file to store downloaded books into
    def output(self):
        output_dir = 'data/downloads'
        # Ensure the directory exists
        os.makedirs(output_dir, exist_ok=True)
        return luigi.LocalTarget(f'{output_dir}/{self.FileID}.txt')

    # Download a specific listed book, remove special characters, and change to lowercase
    def run(self):
        with self.input().open('r') as i:
            url = i.read().splitlines()[self.FileID]

            with io.open(self.output().path, 'w', encoding='utf-8') as f:
                book = requests.get(url, timeout=60)
                book_text = book.text

                for char in self.replace_list:
                    book_text = book_text.replace(char, ' ')

                book_text = book_text.lower()
                f.write(book_text)

class CountWords(luigi.Task):
    FileID = luigi.IntParameter(default=0)
    # Obtain a specific downloaded book as an input
    def requires(self):
        return DownloadBooks(FileID=self.FileID)

    # Specify a local pickle file to store most frequent words with counts as binary
    def output(self):
        output_dir = 'data/counts'
        # Ensure the directory exists
        os.makedirs(output_dir, exist_ok=True)
        file_path = f'{output_dir}/count_{self.FileID}.pickle'
        return luigi.LocalTarget(file_path, format=luigi.format.Nop)

    # Generate word frequency for the downloaded book and store as a binary
    def run(self):
        input_path = self.input().path
        with io.open(input_path, 'r', encoding='utf-8') as i:
            word_count = Counter(i.read().split())
            with io.open(self.output().path, 'wb') as f:
                pickle.dump(word_count, f)

# Set global default parameters for number of books to download and number of most frequent words to report
class GlobalParams(luigi.Config):
    NumberBooks = luigi.IntParameter(default=10)
    NumberTopWords = luigi.IntParameter(default=500)

class TopWords(luigi.Task):
    # Retreive list of top downloaded books, download a specified number of books
    # Obtain count summary for all books as binary input array
    def requires(self):
        inputs = []
        for i in range(GlobalParams().NumberBooks):
            inputs.append(CountWords(FileID=i))
        return inputs

    # Specify local file to store count summaries into
    def output(self):
        return luigi.LocalTarget('data/summary.txt')

    # Retreive summary counts for each book from stored binaries
    # Merge into a combined summary and write into the destination file
    def run(self):
        total_count = Counter()

        for input in self.input():
            with input.open('rb') as i:
                nextCounter = pickle.load(i)
                total_count += nextCounter

            with io.open(self.output().path, 'w', encoding='utf-8') as f:
                for item in total_count.most_common(GlobalParams().NumberTopWords):
                    f.write('{0: <15}{1}\n'.format(*item))

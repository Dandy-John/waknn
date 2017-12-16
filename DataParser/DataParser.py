import csv
import operator
import re
from collections import defaultdict
from stemmer import PorterStemmer
from sys import argv

class DataParse(object):
    def __init__(self, word_size=100, document_size=1000):
        self.words = set()
        self.documents = []
        self.labels = []
        self.word_size = word_size
        self.document_size = document_size
        self.origin_documents = []

    # load data from external csv file and prepare some list for later use
    def load(self, filename):
        with open(filename) as file:
            data = csv.reader(file)
            setData = list(data)

            for index in range(1, len(setData)):
                label = str(setData[index][2])[:-1].split(',')

                if len(label) == 1 and label[0] is not '':
                    self.labels.append(label[0])
                    self.origin_documents.append(re.sub(r'[^\w\s]', "", setData[index][5]).lower())

                    if len(self.labels)== self.document_size:
                        return
    # split document to stemmed words
    def splitDocumentToWords(self):
        stemmer = PorterStemmer()

        for index in range(0, len(self.origin_documents)):
            document = self.origin_documents[index]
            self.origin_documents[index] = [stemmer.stem(x, 0, len(x) - 1) for x in document.split()]

    # choose words from documents as words array
    def chooseWords(self):
        temp = defaultdict(int)

        for document in self.origin_documents:
            for word in document:
                temp[word] += 1

        sorted_temp = sorted(temp.items(), key=operator.itemgetter(1))
        sorted_temp.reverse()

        self.words = [x[0] for x in sorted_temp[:self.word_size]]

    # calculate word occurance to build document array
    def buildDocument(self):

        for index in range(0, len(self.origin_documents)):
            document = self.origin_documents[index]
            self.documents.append([document.count(word) for word in self.words])

        zeros = [i for i, value in enumerate(self.documents) if sum(value) == 0]

        for value in zeros[::-1]:
            del self.labels[value]
            del self.documents[value]

    # normalize document array so that sum of each row is 1
    def normalize(self):
        for index in range(0, len(self.documents)):
            row_sum = sum(self.documents[index])
            self.documents[index] = [float(format(x / row_sum, '.10f')) for x in self.documents[index]]

    # output word array to file
    def output(self, dest):
        with open(dest + "/documents.txt", 'wt') as f:
            for index, document in enumerate(self.documents):
                f.write(str(index) + " ")
                f.write(self.labels[index] + " ")
                f.write(' '.join(str(e) for e in document) + "\n")

        with open(dest + "/words.txt", 'wt') as f:
            f.write(" ".join(self.words))


def getopts(argv):
    opts = {}  # Empty dictionary to store key-value pairs.
    while argv:  # While there are arguments left to parse...
        if argv[0][0] == '-':  # Found a "-name value" pair.
            opts[argv[0]] = argv[1]  # Add key and value to the dictionary.
        argv = argv[1:]  # Reduce the argument list by copying it starting from index 1.
    return opts


if __name__ == '__main__':
    my_args = getopts(argv)

    word_size = int(my_args.get('-ws',100))
    document_size = int(my_args.get('-ds',1000))
    input = my_args.get('-i', 'reuters.csv')
    dest = my_args.get('-o', '.')

    parser = DataParse(word_size, document_size)
    parser.load(input)
    parser.splitDocumentToWords()
    parser.chooseWords()
    parser.buildDocument()
    parser.normalize()
    parser.output(dest)

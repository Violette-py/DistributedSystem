from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

import os

from math import log

from preprocess import count_file
from preprocess import clean_text
from preprocess import tokenize

from constants import DOC_FOLDER

DOCS_NUM = count_file(DOC_FOLDER)

class TFIDF(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.get_words, 
                reducer=self.calc_words_freq_per_doc
            ),
            MRStep(
                mapper=self.get_docs, 
                reducer=self.calc_total_words_per_doc
            ),
            MRStep(
                mapper=self.get_words_from_all_docs,
                reducer=self.calc_words_freq_all_docs
            ),
            MRStep(
                mapper=self.get_tf_idf,
                reducer=self.generate_tf_idf_doc
            )
        ]

    def splitter(self, line):
        cleaned_words = clean_text(line)
        tokenized_words = tokenize(cleaned_words)
        for word in tokenized_words.split():
            yield word

    # => (word, doc), 1
    def get_words(self, _, line):
        # doc = jobconf_from_env("map.input.file")
        doc = os.environ['map_input_file']
        doc = doc.replace(f'file://{DOC_FOLDER}/', '')
        for word in self.splitter(line):
            yield (word, doc), 1

    # => (word, doc), Nw
    def calc_words_freq_per_doc(self, word_doc, occurences):
        yield word_doc, sum(occurences)

    # => doc, (word, Nw)
    def get_docs(self, word_doc, freq):
        word, doc = word_doc  
        yield doc, (word, freq)

    # => (word, doc), (Nw, N)
    # Nw = occurences of word in doc
    # N = total_words in doc
    def calc_total_words_per_doc(self, doc, word_freqs):
        words = []
        freqs = []
        nwords = 0
        for word_freq in word_freqs:
            word, freq = word_freq
            nwords += freq
            words.append(word)
            freqs.append(freq)
            
        for i in range(len(words)):
            yield (words[i], doc), (freqs[i], nwords)
    
    # => word, (doc, Nw, N)
    def get_words_from_all_docs(self, word_doc, freq_nwords):
        word, doc = word_doc
        freq, nwords = freq_nwords
        yield word, (doc, freq, nwords)

    # => (word, doc), (Nw, N, Yw)
    def calc_words_freq_all_docs(self, word, doc_freq_nwords):
        ndocs = 0   # Yw
        docs = []
        freqs = []
        nwords = []
        
        for dfn in doc_freq_nwords:
            ndocs += 1
            docs.append(dfn[0])
            freqs.append(dfn[1])
            nwords.append(dfn[2])
        
        for i in range(len(docs)):
            yield (word, docs[i]), (freqs[i], nwords[i], ndocs)

    # => word, (doc, tfidf)
    def get_tf_idf(self, word_doc, freq_nwords_ndocs):
        word, doc = word_doc
        Nw, N, Yw = freq_nwords_ndocs
        tf_idf = (Nw / N) * log(DOCS_NUM / (Yw + 1))
        yield word, (doc, tf_idf)
        
    # => word, [(doc1, tfidf1), (doc2, tfidf2) ... ]
    def generate_tf_idf_doc(self, word, doc_tfidfs):
        result = list(doc_tfidfs)
        yield word, result

if __name__ == "__main__":
    TFIDF.run()
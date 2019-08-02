import numpy as np
import os
import pandas as pd

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from keras import backend as K
K.tensorflow_backend._get_available_gpus()

from keras.models import load_model
from keras.preprocessing.sequence import pad_sequences
import deepcut
import string
import re
import gensim


class NERPOS_Parser:
    def __init__(self):
        # ::Hard coded char lookup ::
        self.char2Idx = {"PADDING": 0, "UNKNOWN": 1}
        for c in " 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.,-_()[]{}!?:;#'\"/\\%$`&=*+@^~|กขฃคฅฆงจฉชซฌญฎฏฐฑฒณดตถทธนบปผฝพฟภมยรฤลฦวศษสหฬอฮฯะัาำิีึืฺุู฿เแโใไๅๆ็่้๊๋์ํ๎๏๐๑๒๓๔๕๖๗๘๙๚๛":
            self.char2Idx[c] = len(self.char2Idx)
        # :: Hard coded case lookup ::
        self.case2Idx = {'numeric': 0, 'allLower': 1, 'allUpper': 2, 'initialUpper': 3, 'other': 4, 'mainly_numeric': 5,
                         'contains_digit': 6, 'PADDING_TOKEN': 7}

    def load_models(self, loc=None):
        if not loc:
            loc = os.path.join(os.path.expanduser('~'), '.ner_model')
        self.model = load_model(os.path.join(loc, "model.h5"))
        # loading word2Idx
        self.word2Idx = np.load(os.path.join(loc, "word2Idx.npy"), allow_pickle=True).item()
        # loading idx2Label
        self.idx2Label = np.load(os.path.join(loc, "idx2Label.npy"), allow_pickle=True).item()

    def getCasing(self, word, caseLookup):
        casing = 'other'
        numDigits = 0
        for char in word:
            if char.isdigit():
                numDigits += 1
        digitFraction = numDigits / float(len(word))
        if word.isdigit():  # Is a digit
            casing = 'numeric'
        elif digitFraction > 0.5:
            casing = 'mainly_numeric'
        elif word.islower():  # All lower case
            casing = 'allLower'
        elif word.isupper():  # All upper case
            casing = 'allUpper'
        elif word[0].isupper():  # is a title, initial char upper, then all lower
            casing = 'initialUpper'
        elif numDigits > 0:
            casing = 'contains_digit'
        return caseLookup[casing]

    def createTensor(self, sentence, word2Idx, case2Idx, char2Idx):
        unknownIdx = word2Idx['UNKNOWN_TOKEN'].index

        wordIndices = []
        caseIndices = []
        charIndices = []

        for word, char in sentence:
            word = str(word)
            if word in word2Idx:
                wordIdx = word2Idx[word].index
            elif word.lower() in word2Idx:
                wordIdx = word2Idx[word.lower()].index
            else:
                wordIdx = unknownIdx
            charIdx = []
            for x in char:
                if x in char2Idx.keys():
                    charIdx.append(char2Idx[x])
                else:
                    charIdx.append(char2Idx['UNKNOWN'])
            wordIndices.append(wordIdx)
            caseIndices.append(self.getCasing(word, case2Idx))
            charIndices.append(charIdx)

        return [wordIndices, caseIndices, charIndices]

    def addCharInformation(self, sentence):
        return [[word, list(str(word))] for word in sentence]

    def padding(self, Sentence):
        Sentence[2] = pad_sequences(Sentence[2], 70, padding='post')
        return Sentence

    def replaceSpecialCharacterWithTag(self, text_list):
        replace_ls = []
        for word in text_list:
            word = word.replace("(", "<left_parenthesis>")
            word = word.replace(")", "<right_parenthesis>")
            word = word.replace("-", "<minus>")
            word = word.replace("=", "<equal>")
            word = word.replace(",", "<comma>")
            word = word.replace("&", "<ampersand>")
            word = word.replace("'", "<quotation>")
            word = word.replace("\"", "<quotation>")
            word = word.replace("“", "<quotation>")
            word = word.replace("”", "<quotation>")
            word = word.replace("’", "<quotation>")
            word = word.replace("‘", "<quotation>")
            word = word.replace(";", "<semi_colon>")
            word = word.replace("+", "<plus>")
            word = word.replace("!", "<exclamation>")
            word = word.replace("?", "<question_mark>")
            word = word.replace("/", "<slash>")
            word = word.replace(":", "<colon>")
            word = word.replace(".", "<full_stop>")
            word = word.replace("$", "<dollar>")
            word = word.replace(" ", "<space>")
            word = word.replace("\t", "<space>")
            replace_ls.append(word)
        return replace_ls

    def treatValuesOfNumericByComma(self, x):
        nemeric_str = string.digits
        row_ls = []
        buffer_numeric_ls = []
        for token in x:
            if (token.replace(".", "").isdecimal()) | (token.isdecimal()) | (token == ","):
                buffer_numeric_ls.append(token)
            elif (len(buffer_numeric_ls) == 1) and (token == ","):
                for each_sharded in buffer_numeric_ls:
                    row_ls.append(each_sharded)
                    del buffer_numeric_ls[:1]
                    row_ls.append(token)

            elif (len(buffer_numeric_ls) == 1) and (token != ","):

                for each_sharded in buffer_numeric_ls:
                    row_ls.append("num")
                    del buffer_numeric_ls[:1]
                    row_ls.append(token)

            elif len(buffer_numeric_ls) >= 3:
                sharded = "".join(buffer_numeric_ls)
                match_values_str = re.match('^[0-9]*([,.][0-9]*)*([,.][0-9]*)?$', sharded)  # รอใส่ Check RegEx
                if not match_values_str:
                    for each_sharded in buffer_numeric_ls:
                        row_ls.append(each_sharded)
                    del buffer_numeric_ls[:3]
                    row_ls.append(token)
                else:
                    row_ls.append("num")
                    del buffer_numeric_ls[:3]
                    row_ls.append(token)
            else:
                row_ls.append(token)
        return row_ls

    def get_NER(self, ner):
        ner_list = []
        temp = []
        for i in range(len(ner)):

            if (ner[i][2]) == "O":
                if len(temp) != 0:
                    ner_list.append("".join(temp))
                    temp = []
                else:
                    temp = []
            else:
                temp.append(ner[i][0])
        return ner_list

    def cur_predict(self, words):
        #         dict_path = "/apps/ds_sandbox/projects/NExT/data_dict/dict_len.pkl"
        #         dict_words = pd.read_pickle(dict_path).word.values
        #         Sentence = words = deepcut.tokenize(Sentence,custom_dict=list(dict_words))
        Sentence = self.replaceSpecialCharacterWithTag(words)
        Sentence = processed_word = self.treatValuesOfNumericByComma(Sentence)
        Sentence = self.addCharInformation(Sentence)
        Sentence = self.padding(self.createTensor(Sentence, self.word2Idx, self.case2Idx, self.char2Idx))
        tokens, casing, char = Sentence
        tokens = np.asarray([tokens])
        casing = np.asarray([casing])
        char = np.asarray([char])
        pred = self.model.predict([tokens, char], verbose=False)[0]
        pred = pred.argmax(axis=-1)
        pred = [self.idx2Label[x].strip() for x in pred]
        return self.get_NER(list(zip(words, processed_word, pred)))

    def predict(self, words):
        Sentence = self.addCharInformation(words)
        Sentence = self.padding(self.createTensor(Sentence, self.word2Idx, self.case2Idx, self.char2Idx))
        tokens, casing, char = Sentence
        tokens = np.asarray([tokens])
        casing = np.asarray([casing])
        char = np.asarray([char])
        pred = self.model.predict([tokens, char], verbose=False)[0]
        pred = pred.argmax(axis=-1)
        pred = [self.idx2Label[x].strip() for x in pred]
        return self.get_NER(list(zip(words, words, pred)))

    def POS_predict(self, words):
        Sentence = self.replaceSpecialCharacterWithTag(words)
        Sentence = processed_word = self.treatValuesOfNumericByComma(Sentence)
        Sentence = self.addCharInformation(Sentence)
        Sentence = self.padding(self.createTensor(Sentence, self.word2Idx, self.case2Idx, self.char2Idx))
        tokens, casing, char = Sentence
        tokens = np.asarray([tokens])
        casing = np.asarray([casing])
        char = np.asarray([char])
        pred = self.model.predict([tokens, char], verbose=False)[0]
        pred = pred.argmax(axis=-1)
        pred = [self.idx2Label[x].strip() for x in pred]
        return list(zip(words, processed_word, pred))


### Load model ###
cur = NERPOS_Parser()
cur.load_models("resources/models/cur_300d_70char")
dat = NERPOS_Parser()
dat.load_models("resources/models/dat_300d_70char")
cur = NERPOS_Parser()
cur.load_models("resources/models/cur_300d_70char")
org = NERPOS_Parser()
org.load_models("resources/models/org_300d_70char")
per = NERPOS_Parser()
per.load_models("resources/models/per_300d_70char")
POS = NERPOS_Parser()
POS.load_models("resources/models/POS_300d_70char")


def NLP_predict(Sentence):
    if Sentence.strip() != "":
        dict_path = "resources/data_dict/dict_len.pkl"
        dict_words = pd.read_pickle(dict_path).word.values
        Sentence = deepcut.tokenize((str(Sentence)), custom_dict=list(dict_words))
        return Sentence, POS.POS_predict(Sentence), cur.cur_predict(Sentence), org.predict(Sentence), per.predict(
            Sentence), dat.predict(Sentence)

def news_predict_NER(news):
    news[['tokenized','POS','sentence','NER_cur','NER_org','NER_per','NER_dat']]  = news['body'].apply(lambda x : pd.Series(NLP_predict(x)))
    return news
from collections import defaultdict

from konlpy.tag import Okt
from sklearn.feature_extraction.text import TfidfVectorizer


# 키워드 필터
def read_stopwords(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read().split("\n")


class OktTFIDFExtract:
    keyword_filter = []
    stopword_file_path = "./stopwords.txt"

    def __init__(self, keyword_filter, stopword_file_path):
        self.keyword_filter = keyword_filter
        self.stopword_file_path = stopword_file_path

    # OKT 토크나이저
    def okt_tokenizer(self, text):
        okt = Okt()
        pos_res = okt.pos(text, norm=True, stem=True)
        pos_res = [token for token, pos in pos_res if pos in self.keyword_filter[0]]
        stop_word = read_stopwords(self.stopword_file_path)
        pos_res = [word for word in pos_res if not word in stop_word]
        return pos_res

    # 불용어 사전 파일 읽기

    # TF-IDF 키워드 추출
    def extract_keywords(self, text):
        vectorizer = TfidfVectorizer(tokenizer=self.okt_tokenizer)
        vectorizer.fit(text)
        matrix = vectorizer.fit_transform(text)

        # 단어 사전: {"token": id}
        vocabulary_word_id = defaultdict(int)

        for idx, token in enumerate(vectorizer.get_feature_names_out()):
            vocabulary_word_id[token] = idx

        # 특징 추출 결과: {"token": value}
        result = defaultdict(str)

        for token in vectorizer.get_feature_names_out():
            result[token] = matrix[0, vocabulary_word_id[token]]

        # 내림차순 (중요도 high) 기준 정렬
        result = sorted(result.items(), key=lambda item: item[1], reverse=True)

        return [{"keyword": token, "value": round(value * 10, 1)} for token, value in result if not value < 0.1]
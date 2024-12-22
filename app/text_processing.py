import pandas as pd
from nltk.corpus import stopwords
import re
# import pymystem3
import spacy
import pyaspeller
import time

class Preprocessor:
    def __init__(self, stop_words=None, abbreviations=None):
        #print('preproc init')
        self.nlp = spacy.load("ru_core_news_sm")
        self.stop_words = stop_words if stop_words else set(stopwords.words('russian'))
        self.abbreviations = abbreviations
    
    def clean_text(self, text):
        # Удаление знаков препинания чисел и ссылок
        #print('clean_text')
        text = re.sub(r'http\S+|www\S+|[^\w\s]', '', text)
        #print('re')
        # Приведение текста к нижнему регистру
        text = text.lower()
        #print('lower')
    
        # if self.abbreviations: text = self.replace_abbreviations(text)
        #print('abbreviations')

        text = self.correct_spelling(text)

        tokens = self.lemmatize_and_remove_stopwords(text)
        
        return tokens
    
    def replace_abbreviations(self, text):
        for abbr, full_form in self.abbreviations.items():
            text = text.replace(abbr, full_form)
        return text

    # def lemmatize(text):
    #     mystem = pymystem3.Mystem()  
    #     return ''.join(mystem.lemmatize(text)).strip()
    
    def lemmatize_and_remove_stopwords(self, text):
        # Применение spaCy к тексту
        doc = self.nlp(text)
        # Лемматизация и удаление стоп-слов
        tokens = [token.lemma_ for token in doc if token.lemma_ not in self.stop_words and not token.is_punct and 2 <= len(token.text) <= 15]
        return tokens

    def correct_spelling(self, text): 
        try:
            checker = pyaspeller.YandexSpeller(lang='ru')  
            errors = checker.spell(text)
            for error in errors: 
                text = text.replace(error['word'], error['s'][0])
            return text
        except:
            print(f"Ошибка correct_spelling. Оставляю текст без изменений.") 
            return text 
        


from gensim.models import KeyedVectors
import ast
import numpy as np

class Vectorizer:
    def __init__(self, model, max_review_len=100):
        #print('vec init')
        self.max_review_len = max_review_len
        # Создание модели KeyedVectors
        vector_size = len(next(iter(model.values())))  # Определение размерности векторов
        self.model = KeyedVectors(vector_size=vector_size)
        # Добавление векторов в KeyedVectors
        self.model.add_vectors(list(model.keys()), list(model.values()))
    
    def vectorize_text(self, token_list):
        unk = self.model['<unk>']
        text_embeddings = []

        if not isinstance(token_list, list):
            token_list = ast.literal_eval(token_list)
        
        for token in token_list:
            # Проверяем, есть ли токен в модели, и добавляем его векторы
            if token in self.model:
                embedding = self.model[token]
            else:
                embedding = unk  # Если слова нет, используем вектор для <unk>
            text_embeddings.append(embedding)
        
        # Дополняем или обрезаем отзывы для фиксированной длины max_review_len
        l = len(text_embeddings)
        if l > self.max_review_len:
            text_embeddings = text_embeddings[:self.max_review_len]
        else:
            text_embeddings.extend([self.model['<pad>']] * (self.max_review_len - l))
        
        return text_embeddings


class SentimentAnalyzer:
    def __init__(self, model, vector_size=300, max_review_len=100): # x - векторизированный текст
        self.model = model
        self.vector_size = vector_size
        self.max_review_len = max_review_len
        self.mapping = {0: 'Негативный', 1: 'Нейтральный', 2:'Положительный'}
    
    def analyze(self, x):
        X = np.array(x).reshape(-1, self.vector_size * self.max_review_len)
        y_pred = self.model.predict(X)
        return y_pred[0].item() 

from sklearn.metrics.pairwise import cosine_similarity 
import time 
from concurrent.futures import ThreadPoolExecutor
    
class KeywordExtractor:
    def __init__(self, keywords, model, preprocessor, vectorizer, expand_n=5, similarity_threshold=0.5):
        # Создание модели KeyedVectors
        print('start KEYWORD')
        start_time = time.time()
        vector_size = len(next(iter(model.values())))  # Определение размерности векторов
        self.model = KeyedVectors(vector_size=vector_size)
        # Добавление векторов в KeyedVectors
        self.model.add_vectors(list(model.keys()), list(model.values()))
        self.preprocessor = preprocessor
        self.vectorizer = vectorizer
        self.expand_n = expand_n
        self.similarity_threshold = similarity_threshold

        # self.keywords = [self.preproccessor.clean_text(word) for word in keywords]
        with ThreadPoolExecutor(max_workers=10) as executor:
            self.keywords = list(executor.map(self.preprocessor.clean_text, keywords))
    
        #self.keywords = pd.Series(keywords).apply(self.preproccessor.clean_text).tolist()
        expanded_keywords = {}

        for keyword in self.keywords:
            # Объединяем подслова в строку, чтобы использовать как ключ
            combined_keyword = ' '.join(keyword)

            # Ищем ближайшие слова для каждого подслова в ключевом слове
            for subword in keyword:
                # Ищем ближайшие слова для данного ключевого слова
                try:
                    similar_words = self.model.most_similar(subword, topn=self.expand_n)
                    
                    # Фильтруем слова по порогу сходства
                    filtered_words = [word for word, similarity in similar_words if similarity >= self.similarity_threshold]
                    filtered_words.append(subword)
                    # Добавляем в словарь расширенных ключевых слов
                    # Используем объединённое ключевое слово как ключ
                    if combined_keyword in expanded_keywords:
                        expanded_keywords[combined_keyword].extend(filtered_words)
                    else:
                        expanded_keywords[combined_keyword] = filtered_words
                except KeyError:
                    print(f"Слово '{subword}' отсутствует в модели.")

        # Удаляем дубликаты в значениях
        for key in expanded_keywords:
            expanded_keywords[key] = list(set(expanded_keywords[key]))
        self.expanded_keywords = expanded_keywords
        # Предварительная векторизация ключевых слов для каждой темы
        self.expanded_keywords_vectors = {
            theme: np.mean(np.array(self.vectorizer.vectorize_text(keywords)), axis=0)
            for theme, keywords in self.expanded_keywords.items()
            if keywords  # Проверка наличия векторов
        }
        print('end KEYWORD')
        end_time = time.time()
        print(f'Время выполнения: {end_time-start_time} секунд')

        
    def match_review_to_themes(self, review_vector):
        matched_themes = []
        #review_vector_mean = np.mean(review_vector, axis=0)
        review_vector_mean = np.mean(review_vector, axis=0).reshape(1, -1)
        for theme, theme_vector in self.expanded_keywords_vectors.items():
            similarity = cosine_similarity(review_vector_mean, theme_vector.reshape(1, -1))[0][0]
            if similarity > self.similarity_threshold:
                matched_themes.append(theme)

        return matched_themes

import streamlit as st
import pandas as pd
import psycopg2
# Импортируйте ваши классы
from text_processing import Preprocessor, Vectorizer, SentimentAnalyzer, KeywordExtractor
from nltk.corpus import stopwords
import time
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import joblib
import json
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import seaborn as sns

sentiment_model = joblib.load("C:\\Users\\an23m\\course_work\\svm_model.pkl")
# Загрузка сохраненного словаря с векторами
combined_vectors_array = np.load("C:\\Users\\an23m\\course_work\\combined_vectors.npy", allow_pickle=True).item()

stop_words = set(stopwords.words('russian'))
words_to_remove = {'не', 'всегда', 'лучше', 'никогда', 'хорошо', 'нет', 'нельзя', 'можно'}
for word in words_to_remove:
    stop_words.discard(word)

abbreviations = {
    "хз": "не знаю",
    "незн": "не знаю",
    "щас": "сейчас",
    "чё": "что",
    "спс": "спасибо",
    "щас": "сейчас",
    "чот": "что-то",
    "пж": "пожалуйста",
    "норм": "нормально",
    "кст": "кстати",
    "мб": "может быть",
    "тк": "так как",
    "всм": "в смысле",
    "чел": "человек",
    "блин": "ужас",
    "плз": "пожалуйста"
}

def get_db_connection_params():
    st.subheader("Параметры подключения к базе данных")
    db_host = st.text_input("Хост базы данных", "localhost")
    db_name = st.text_input("Имя базы данных", "azs")
    db_user = st.text_input("Пользователь базы данных", "postgres")
    db_password = st.text_input("Пароль базы данных", "f8ysz789", type="password")
    return db_host, db_name, db_user, db_password

def get_reviews(db_host, db_name, db_user, db_password):
    # Установка соединения с базой данных
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )
    cur = conn.cursor()

    df = pd.read_sql_query("SELECT * FROM testing.azs_review order by object_id", conn) # убрать лимит

    cur.close()
    conn.close()
    return df

def get_keywords(db_host, db_name, db_user, db_password):
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )
    cur = conn.cursor()

    themes = pd.read_sql_query("select title from testing.s_azs_categ order by title", conn)

    cur.close()
    conn.close()
    keywords = themes['title'].tolist()
    return keywords

def analyze_reviews(df, preprocessor, vectorizer, sentiment_analyzer, keyword_extractor):
    # Проверка и создание столбца 'cleaned_text'
    if 'cleaned_text' not in df.columns:
        texts = df['comment_text'].tolist()  # Преобразуем столбец в список
        with ThreadPoolExecutor(max_workers=10) as executor:
            cleaned_texts = list(executor.map(preprocessor.clean_text, texts))
        df['cleaned_text'] = cleaned_texts
        print("Предобработка текста завершена и добавлена в 'cleaned_text'.")

    # Проверка и создание столбца 'vectorized_text'
    if 'vectorized_text' not in df.columns:
        df['vectorized_text'] = df['cleaned_text'].apply(lambda x: vectorizer.vectorize_text(x))
        print("Векторизация текста завершена и добавлена в 'vectorized_text'.")

    # Проверка и создание столбца 'sentiment'
    if 'sentiment' not in df.columns:
        df['sentiment'] = df['vectorized_text'].apply(lambda x: sentiment_analyzer.analyze(x))
        print("Анализ тональности завершен и добавлен в 'sentiment'.")
        # Если нужно получить названия классов вместо числовых меток
        df['sentiment_label'] = df['sentiment'].replace(sentiment_analyzer.mapping) # мб убрать

    # Проверка и создание столбца 'keywords'
    if 'keywords' not in df.columns:
        df['keywords'] = df['vectorized_text'].apply(lambda x: keyword_extractor.match_review_to_themes(x))
        print("Выделение ключевых слов завершено и добавлено в 'keywords'.")


def export_to_csv(dataframe, filename='processed_reviews.csv'):
    dataframe.to_csv(filename, index=False)
    st.download_button(
        label="Скачать CSV",
        data=dataframe.to_csv(index=False).encode('utf-8'),
        file_name=filename,
        mime='text/csv'
    )

def main():
    st.title("Интеллектуальная система анализа отзывов")

    option = st.radio(
        "Выберите источник данных:",
        ('Подключение к базе данных', 'Загрузка CSV-файла')
    )

    # Используем сессию для хранения DataFrame
    if 'reviews_df' not in st.session_state:
        st.session_state.reviews_df = None

    if option == 'Подключение к базе данных':
        db_host, db_name, db_user, db_password = get_db_connection_params()
        if st.button("Подключиться"):
            st.session_state.reviews_df = get_reviews(db_host, db_name, db_user, db_password)
            st.session_state.keywords = get_keywords(db_host, db_name, db_user, db_password)

    elif option == 'Загрузка CSV-файла':
        uploaded_file = st.file_uploader("Выберите CSV-файл", type="csv")
        if uploaded_file is not None:
            st.session_state.reviews_df = pd.read_csv(uploaded_file)
            with open("C:\\Users\\an23m\\course_work\\keywords.json", 'r', encoding='utf-8') as file:
                st.session_state.keywords = json.load(file)
    # Проверка, что данные успешно загружены
    if st.session_state.reviews_df is not None:
        st.subheader("Все отзывы")
        st.dataframe(st.session_state.reviews_df)

        preprocessor = Preprocessor(stop_words) # ,abbreviations
        vectorizer = Vectorizer(combined_vectors_array)
        sentiment_analyzer = SentimentAnalyzer(sentiment_model)
        # Проверка, создан ли уже keyword_extractor
        if 'keyword_extractor' not in st.session_state:
            st.session_state.keyword_extractor = KeywordExtractor(
                st.session_state.keywords,
                combined_vectors_array,
                preprocessor,
                vectorizer
            )
        if 'analysis_done' not in st.session_state:
            st.session_state.analysis_done = False    
                # Кнопка для анализа всех отзывов
        if st.button("Анализировать все отзывы"):
            # Инициализация классов
            with st.spinner("Идет анализ отзывов..."): # сделать прогрес бар: предобработка, векторизация, тональность, ключевые слова
                start_time = time.time()  # Запись времени начала
                analyze_reviews(st.session_state.reviews_df, preprocessor, vectorizer, sentiment_analyzer, st.session_state.keyword_extractor)  # Ваш анализ здесь
                end_time = time.time()
                st.text(f'Время выполнения: {end_time-start_time} секунд')
            st.session_state.analysis_done = True

        if st.session_state.analysis_done:
        # Отображение результатов анализа
            st.subheader("Результаты анализа")
            st.dataframe(st.session_state.reviews_df.head(100))
            # ВЕРНУТЬ st.dataframe(st.session_state.reviews_df[['comment_text', 'cleaned_text', 'sentiment', 'keywords']].head(100)) # 100 чтобы сильно не нагружать память, основной анализ по визуализации будет показан
            # Экспорт в CSV
            export_to_csv(st.session_state.reviews_df)   
            with st.expander("Посмотреть графики", expanded=False):
                if st.button("Облако слов"):
                    # positive_reviews = ' '.join(st.session_state.reviews_df[st.session_state.reviews_df['sentiment'] == 2]['cleaned_text'])
                    positive_reviews = ' '.join(
                    [' '.join(tokens) for tokens in st.session_state.reviews_df[st.session_state.reviews_df['sentiment'] == 2]['cleaned_text']]
                )
                    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(positive_reviews)

                    # Отображаем облако слов
                    plt.figure(figsize=(10, 5))
                    plt.imshow(wordcloud, interpolation='bilinear')
                    plt.axis("off")
                    plt.title("Облако слов для положительных отзывов")
                    st.pyplot(plt)

                    # negative_reviews = ' '.join(st.session_state.reviews_df[st.session_state.reviews_df['sentiment'] == 0]['cleaned_text'])
                    # wordcloud = WordCloud(width=800, height=400, background_color='white').generate(negative_reviews)

                    # # Отображаем облако слов
                    # plt.figure(figsize=(10, 5))
                    # plt.imshow(wordcloud, interpolation='bilinear')
                    # plt.axis("off")
                    # plt.title("Облако слов для негативных отзывов")
                    # st.pyplot(plt)

                if st.button("Гистограмма тональности"):
                    plt.figure(figsize=(8, 5))
                    sns.countplot(data=st.session_state.reviews_df, x='sentiment', palette='Set2')
                    plt.title('Распределение тональности отзывов')
                    plt.xlabel('Тональность')
                    plt.ylabel('Количество')
                    st.pyplot(plt)

                if st.button("Частотное распределение тональности по категориям"):
                    # Разворачиваем колонку keywords, чтобы каждая категория была в отдельной строке
                    exploded_df = st.session_state.reviews_df.explode('keywords')
                    sentiment_distribution = exploded_df.groupby(['keywords', 'sentiment']).size().unstack(fill_value=0)
                     # учесть что в keywords лежит список, может быть несколько слов
                    sentiment_distribution.plot(kind='bar', stacked=True, figsize=(10, 6))
                    plt.title('Распределение отзывов по категориям и тональности')
                    plt.xlabel('Категория')
                    plt.ylabel('Количество отзывов')
                    plt.xticks(rotation=45)
                    plt.legend(title='Тональность', labels=['Отрицательная', 'Нейтральная', 'Положительная'])
                    plt.tight_layout()
                    st.pyplot(plt)

    # можно например добавить результаты о том какое количество положительных и отрицательных отзывов для каждой азс. какие категории наилучшие и наихудшие для каждой азс.

                if st.button("Распределение тональности по регионам"):
                    plt.figure(figsize=(10, 6))
                    sns.countplot(data=st.session_state.reviews_df, x='region', hue='sentiment', palette='coolwarm')
                    plt.title("Распределение тональности по регионам")
                    plt.xlabel("Регион")
                    plt.ylabel("Количество отзывов")
                    st.pyplot(plt)


                # review_counts с информацией о количестве отзывов и проценте негативных отзывов для каждой АЗС

                review_counts = st.session_state.reviews_df.groupby('object_id').agg(
                total_reviews=('sentiment', 'size'),
                negative_reviews=('sentiment', lambda x: (x == 0).sum()),
                neutral_reviews=('sentiment', lambda x: (x == 1).sum()),
                positive_reviews=('sentiment', lambda x: (x == 2).sum()),
                ).reset_index()

                # Вычисляем процент отзывов для каждой АЗС
                review_counts['negative_ratio'] = review_counts['negative_reviews'] / review_counts['total_reviews'] * 100
                review_counts['neutral_ratio'] = review_counts['neutral_reviews'] / review_counts['total_reviews'] * 100
                review_counts['positive_ratio'] = review_counts['positive_reviews'] / review_counts['total_reviews'] * 100

                if st.button("Список АЗС с наибольшим количеством негативных отзывов"):
                    # Сортируем по проценту негативных отзывов в порядке убывания
                    top_negative = review_counts.sort_values(by='negative_ratio', ascending=False).head(20).reset_index()
                    plt.figure(figsize=(10, 6))
                    sns.barplot(data=top_negative, x='object_id', y='negative_ratio', palette='Reds')
                    plt.title("Топ-20 АЗС с наибольшим процентом негативных отзывов")
                    plt.xlabel("АЗС ID")
                    plt.ylabel("Процент негативных отзывов")
                    st.pyplot(plt)

                if st.button("Анализ ключевых проблемных категорий для каждой из проблемных АЗС"):
                    # Фильтрация негативных отзывов и группировка по АЗС и категориям
                    exploded_df = st.session_state.reviews_df.explode('keywords')
                    negative_reviews = exploded_df[exploded_df['sentiment'] == 0]
                    category_counts = negative_reviews.groupby(['object_id', 'keywords']).size().unstack().fillna(0) 
# учесть что в keywords лежит список, может быть несколько слов
                    # Отображение данных для анализа
                    top_negative = review_counts.sort_values(by='negative_ratio', ascending=False).head(20).reset_index()
                    st.write("Категории с наибольшим количеством негативных отзывов для топ-10 АЗС:")
                    st.write(category_counts.loc[top_negative['object_id']])
                
                # добавить что-то с временными рядами ? 


if __name__ == "__main__":
    main()
            
            # сделать requierements file


# def analyze_sentiment_by_azs(reviews_df):
#     # Группируем по АЗС и тональности
#     sentiment_counts = reviews_df.groupby(['objectId', 'sentiment']).size().unstack(fill_value=0)
#     sentiment_counts.columns = ['Negative', 'Neutral', 'Positive']
#     return sentiment_counts

# def analyze_best_worst_categories(reviews_df):
#     # Группируем по АЗС и категориям, считаем количество положительных и отрицательных отзывов
#     category_sentiment_counts = reviews_df.groupby(['objectId', 'category', 'sentiment']).size().unstack(fill_value=0)
    
#     # Находим наилучшие и наихудшие категории по каждой АЗС
#     best_worst_categories = category_sentiment_counts.groupby(level=0).apply(lambda x: x.idxmax().to_frame(name='Best_Category').join(x.idxmin().to_frame(name='Worst_Category')))
    
#     return best_worst_categories, category_sentiment_counts

# def plot_sentiment_distribution_by_azs(sentiment_counts):
#     sentiment_counts.plot(kind='bar', stacked=True, figsize=(10, 6))
#     plt.title('Распределение отзывов по АЗС')
#     plt.xlabel('АЗС')
#     plt.ylabel('Количество отзывов')
#     plt.xticks(rotation=45)
#     plt.legend(title='Тональность')
#     plt.tight_layout()
#     st.pyplot(plt)

# def main():
#     st.title("Анализ отзывов")

#     # Проверка, были ли уже проанализированы отзывы
#     if 'analysis_done' not in st.session_state:
#         st.session_state.analysis_done = False

#     if st.button("Анализировать все отзывы"):
#         # Инициализация классов
#         with st.spinner("Идет анализ отзывов..."):
#             analyze_reviews(st.session_state.reviews_df, preprocessor, vectorizer, sentiment_analyzer, st.session_state.keyword_extractor)
#             st.session_state.analysis_done = True

#     # Отображение результатов анализа
#     if st.session_state.analysis_done:
#         st.subheader("Результаты анализа")
#         st.dataframe(st.session_state.reviews_df[['comment_text', 'cleaned_text', 'sentiment', 'keywords']].head(100))

#         # Анализ отзывов по АЗС
#         sentiment_counts = analyze_sentiment_by_azs(st.session_state.reviews_df)
#         best_worst_categories, category_sentiment_counts = analyze_best_worst_categories(st.session_state.reviews_df)

#         st.subheader("Количество положительных и отрицательных отзывов по АЗС")
#         st.dataframe(sentiment_counts)

#         st.subheader("Наилучшие и наихудшие категории для каждой АЗС")
#         st.dataframe(best_worst_categories)

#         st.subheader("График распределения отзывов по АЗС")
#         plot_sentiment_distribution_by_azs(sentiment_counts)


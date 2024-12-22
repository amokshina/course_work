import streamlit as st
import pandas as pd
import psycopg2
from text_processing import Preprocessor, Vectorizer, SentimentAnalyzer, KeywordExtractor # мои классы
from nltk.corpus import stopwords
import time
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import joblib
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import ast
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from datetime import datetime
import hashlib


logging.basicConfig(level=logging.INFO) 

sentiment_model = joblib.load("C:\\Users\\an23m\\course_work\\app\\model\\svm_model.pkl")
# Загрузка сохраненного словаря с векторами
combined_vectors_array = np.load("C:\\Users\\an23m\\course_work\\app\\model\\combined_vectors.npy", allow_pickle=True).item()

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

def md5(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def get_db_connection_params():
    db_host = st.text_input("Хост базы данных", "localhost")
    db_name = st.text_input("Имя базы данных", "azs")
    db_user = st.text_input("Пользователь базы данных", "postgres")
    db_password = st.text_input("Пароль базы данных", "f8ysz789", type="password")
    return db_host, db_name, db_user, db_password


def get_unprocessed_reviews(db_host, db_name, db_user, db_password):
    try:
        with psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
            ) as conn:
            with conn.cursor() as cursor: 
                df = pd.read_sql_query("""select ar.comment_text from testing.azs_review ar 
                left join testing.azs_review_analysis AS a 
                on md5(ar.comment_text) = a.review_hash
                where ar.date_of_pars = (select max(date_of_pars) from testing.azs_review) and a.review_hash is null""", conn) 
                st.session_state.db_connection = True
                return df
            
    except Exception as e:
        st.error("Не удалось установить соединение с базой данных. Проверьте параметры подключения.")
        st.session_state.db_connection = False
        logging.warning(f"Error with connection or cursor: {e}" )
        return pd.DataFrame() 
    
def parse_string_to_list(string_value):
    # Если значение уже является списком, просто возвращаем его
    if isinstance(string_value, list):
        return string_value
    # Если это строка, пытаемся преобразовать её в список
    elif isinstance(string_value, str):
        try:
            return ast.literal_eval(string_value) if string_value else []
        except (ValueError, SyntaxError):
            return []  # Возвращаем пустой список, если не удается преобразовать строку
    return []

def get_keywords(db_host, db_name, db_user, db_password):
    try:
        with psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
            ) as conn:
            with conn.cursor() as cursor: 
                themes = pd.read_sql_query("select title from testing.s_azs_categ order by title", conn)
                keywords = themes['title'].tolist()
                #cursor.execute(query=)   
                return keywords
    except Exception as e:
        logging.warning(f"Error with connection or cursor: {e}" )
        return []

def analyze_reviews(df, preprocessor, vectorizer, sentiment_analyzer, keyword_extractor):
    # Проверка и создание столбца 'cleaned_text'
    if 'clean_text' not in df.columns:
        texts = df['comment_text'].tolist()  # Преобразуем столбец в список
        with ThreadPoolExecutor(max_workers=10) as executor:
            cleaned_texts = list(executor.map(preprocessor.clean_text, texts))
        df['clean_text'] = cleaned_texts
        print("Предобработка текста завершена и добавлена в 'clean_text'.")

    # Проверка и создание столбца 'vectorized_text'
    if 'vectorized_text' not in df.columns:
        df['vectorized_text'] = df['clean_text'].apply(lambda x: vectorizer.vectorize_text(x))
        print("Векторизация текста завершена и добавлена в 'vectorized_text'.")

    vectorized_texts = df['vectorized_text'].tolist()  # Список векторизованных текстов

     # Параллельная обработка для анализа тональности
    if 'sentiment_score' not in df.columns:
        with ThreadPoolExecutor(max_workers=10) as executor:
            sentiments = list(executor.map(sentiment_analyzer.analyze, vectorized_texts))
        df['sentiment_score'] = sentiments
        print("Анализ тональности завершен и добавлен в 'sentiment'.")
        # Если нужно получить названия классов вместо числовых меток
        df['sentiment'] = df['sentiment_score'].replace(sentiment_analyzer.mapping)

    # Параллельная обработка для извлечения ключевых слов
    if 'keywords' not in df.columns:
        with ThreadPoolExecutor(max_workers=10) as executor:
            keywords = list(executor.map(keyword_extractor.match_review_to_themes, vectorized_texts))
        df['keywords'] = keywords
        print("Выделение ключевых слов завершено и добавлено в 'keywords'.")

    df['review_hash'] = df['comment_text'].apply(md5)

    st.session_state.analysis_done = True   

def load_analyzed_reviews_to_db(db_host, db_name, db_user, db_password, values):
    try:
        with psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
            ) as conn:
            with conn.cursor() as cur: 
                cur.execute('TRUNCATE TABLE "buffer"."azs_review_analysis" CASCADE')
                insert_query = """
                    INSERT INTO buffer.azs_review_analysis (
                        review_hash, comment_text, clean_text, sentiment, sentiment_score, keywords
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                # Выполнение вставки данных
                cur.executemany(insert_query, values)
                cur.execute("select testing.fn_load_analyzed_reviews()")
                
    except Exception as e:
        logging.warning(f"Error with connection or cursor: {e}" )

def get_analyzed_reviews(db_host, db_name, db_user, db_password):
    try:
        with psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
            ) as conn:
            with conn.cursor() as cur: 
                df = pd.read_sql_query("""select ai.object_id, ai.rating as azs_rating, address, region, profession_level_num, 
                                            ar.rating as review_rating, ar.comment_text, ar.comment_time, sentiment, sentiment_score, keywords, clean_text
                                            from testing.azs_info as ai 
                                            join testing.azs_review as ar 
                                            on ai.object_id = ar.object_id 
                                            left join testing.azs_review_analysis as a 
                                            on md5(ar.comment_text) = a.review_hash
                                            where ar.date_of_pars = (select max(date_of_pars) from testing.azs_review)
                                            order by ar.comment_time desc;""", conn) 
                return df
                
    except Exception as e:
        logging.warning(f"Error with connection or cursor: {e}" )
        return pd.DataFrame()



def main():
    st.title("Интеллектуальная система анализа отзывов")
    # Используем сессию для хранения DataFrame
    if 'reviews_df' not in st.session_state:
        st.session_state.reviews_df = None
    if 'analysis_done' not in st.session_state:
        st.session_state.analysis_done = False   
    if 'first_analysis_done' not in st.session_state:
            st.session_state.first_analysis_done = False   

    st.subheader("Подключение к базе данных")

    db_host, db_name, db_user, db_password = get_db_connection_params()
    if st.button("Подключиться"):
        try:
            st.session_state.reviews_df = get_unprocessed_reviews(db_host, db_name, db_user, db_password)
            st.session_state.first_analysis_done = True
        except Exception as e:
            st.error(f"Ошибка при подключении к базе данных: {str(e)}")  
    # Проверка, что данные успешно загружены
    if st.session_state.reviews_df is not None and not st.session_state.reviews_df.empty and st.session_state.db_connection:
        # выводить следующее только если в reviews_df что то лежит
        st.session_state.first_analysis_done = False
        st.subheader("Необработанные отзывы")
        st.dataframe(st.session_state.reviews_df)
        # Кнопка для анализа всех отзывов
        if st.button("Анализировать необработанные отзывы"):
            preprocessor = Preprocessor(stop_words) # ,abbreviations
            vectorizer = Vectorizer(combined_vectors_array)
            sentiment_analyzer = SentimentAnalyzer(sentiment_model)
            # Проверка, создан ли уже keyword_extractor
            st.session_state.keywords = get_keywords(db_host, db_name, db_user, db_password)
            if 'keyword_extractor' not in st.session_state:
                st.session_state.keyword_extractor = KeywordExtractor(
                    st.session_state.keywords,
                    combined_vectors_array,
                    preprocessor,
                    vectorizer
                )
            # Инициализация классов
            with st.spinner("Идет анализ отзывов..."): 
                start_time = time.time()  # Запись времени начала
                analyze_reviews(st.session_state.reviews_df, preprocessor, vectorizer, sentiment_analyzer, st.session_state.keyword_extractor)  # анализ здесь
                end_time = time.time()
                st.text(f'Время выполнения: {end_time-start_time} секунд')
        
        if st.session_state.analysis_done:
            st.subheader("Обработанные отзывы")
            st.dataframe(st.session_state.reviews_df)
            if st.button("Обновить БД"):
                values = [(row['review_hash'], 
                    row['comment_text'], 
                    row['clean_text'], 
                    row['sentiment'], 
                    row['sentiment_score'], 
                    parse_string_to_list(row['keywords'])) 
                    for index, row in st.session_state.reviews_df.iterrows()]
                load_analyzed_reviews_to_db(db_host, db_name, db_user, db_password, values)
                st.success('Успешно обновлено')
                st.session_state.first_analysis_done = True
                
    # if st.session_state.reviews_df is not None and st.session_state.reviews_df.empty and st.session_state.first_analysis_done: 
    #     st.session_state.analysis_done = True    

    if st.session_state.first_analysis_done and st.session_state.db_connection:
    # Отображение результатов анализа
        st.subheader("Результаты анализа")
        if 'analyzed_reviews_df' not in st.session_state:
            st.session_state.analyzed_reviews_df = None
        st.session_state.analyzed_reviews_df = get_analyzed_reviews(db_host, db_name, db_user, db_password)
        cropped_df = st.session_state.analyzed_reviews_df[['comment_time','comment_text', 'sentiment', 'keywords', 'address', 'azs_rating', ]]
        st.dataframe(cropped_df.head(100)) 

        # with st.expander("Посмотреть графики", expanded=True):
        # Список для хранения всех графиков
        if 'figures' not in st.session_state:
            st.session_state.figures = []

        if not st.session_state.figures:
            try:
                st.subheader("Гистограмма тональности")
                fig, ax = plt.subplots(figsize=(10, 6))
                # plt.figure(figsize=(10, 6))
                sns.countplot(data=st.session_state.analyzed_reviews_df, x='sentiment_score', palette='Set2')
                plt.title('Распределение тональности отзывов')
                plt.xlabel('Тональность')
                plt.ylabel('Количество')
                st.pyplot(plt)

                st.session_state.figures.append(fig) # для экспорта
            except:
                pass

            try:
                st.subheader("Частотное распределение тональности по категориям")
                # Разворачиваем колонку keywords, чтобы каждая категория была в отдельной строке
                st.session_state.analyzed_reviews_df['hour'] = st.session_state.analyzed_reviews_df['comment_time'].dt.hour
                exploded_df = st.session_state.analyzed_reviews_df.explode('keywords')
                sentiment_distribution = exploded_df.groupby(['keywords', 'sentiment_score']).size().unstack(fill_value=0)
                
                fig, ax = plt.subplots(figsize=(10, 6))
                sentiment_distribution.plot(kind='bar', stacked=True, ax=ax)
                ax.set_title('Распределение отзывов по категориям и тональности')
                ax.set_xlabel('Категория')
                ax.set_ylabel('Количество отзывов')
                ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
                ax.legend(title='Тональность', labels=['Отрицательная', 'Нейтральная', 'Положительная'])
                plt.tight_layout()
                st.pyplot(plt) # мб поменять на fig

                st.session_state.figures.append(fig)

            except:
                pass

            try:
                st.subheader("Распределение отзывов по категориям и тональности в процентном соотношении")
                sentiment_distribution_percent = sentiment_distribution.div(sentiment_distribution.sum(axis=1), axis=0) * 100

                # Построение графика
                fig, ax = plt.subplots(figsize=(10, 6))
                sentiment_distribution_percent.plot(kind='bar', stacked=True, ax=ax)
                plt.title('Распределение отзывов по категориям и тональности в процентном соотношении')
                plt.xlabel('Категория')
                plt.ylabel('Процент отзывов')
                plt.xticks(rotation=90)
                plt.legend(title='Тональность', labels=['Отрицательная', 'Нейтральная', 'Положительная'])
                plt.tight_layout()
                st.pyplot(plt)

                st.session_state.figures.append(fig)

            except:
                pass
    # можно например добавить результаты о том какое количество положительных и отрицательных отзывов для каждой азс. какие категории наилучшие и наихудшие для каждой азс.
            try:
                st.subheader("Распределение тональности по регионам")
                fig, ax = plt.subplots(figsize=(10, 6))
                sns.countplot(data=st.session_state.analyzed_reviews_df, x='region', hue='sentiment_score', palette='coolwarm')
                plt.title("Распределение тональности по регионам")
                plt.xlabel("Регион")
                plt.xticks(rotation=90)
                plt.ylabel("Количество отзывов")
                st.pyplot(plt)
                st.session_state.figures.append(fig)
            except:
                pass
            
            try:
                st.subheader("Распределение тональности по регионам (в процентах)")
                # Вычисляем количество отзывов по каждому региону и тональности
                region_sentiment_counts = st.session_state.analyzed_reviews_df.groupby(['region', 'sentiment_score']).size().unstack(fill_value=0)

                # Преобразуем количество в проценты для каждого региона
                region_sentiment_percent = region_sentiment_counts.div(region_sentiment_counts.sum(axis=1), axis=0) * 100

                # Сортируем регионы по проценту положительных отзывов (предполагаем, что положительная тональность = 1)
                region_sentiment_percent = region_sentiment_percent.sort_values(by=0, ascending=False)

                # Преобразуем данные для построения графика в нужном формате
                region_sentiment_percent = region_sentiment_percent.reset_index().melt(id_vars='region', var_name='sentiment_score', value_name='percentage')

                # Построение графика с процентами
                fig, ax = plt.subplots(figsize=(10, 6))
                sns.barplot(data=region_sentiment_percent, x='region', y='percentage', hue='sentiment_score', palette='coolwarm')
                plt.title("Распределение тональности по регионам (в процентах)")
                plt.xlabel("Регион")
                plt.ylabel("Процент отзывов")
                plt.xticks(rotation=90)
                # plt.legend(title='Тональность', labels=['Отрицательная', 'Нейтральная', 'Положительная'])
                plt.tight_layout()
                st.pyplot(plt)
                st.session_state.figures.append(fig)

            except:
                pass
            # review_counts с информацией о количестве отзывов и проценте негативных отзывов для каждой АЗС
            try:
                review_counts = st.session_state.analyzed_reviews_df.groupby('object_id').agg(
                total_reviews=('sentiment_score', 'size'),
                negative_reviews=('sentiment_score', lambda x: (x == 0).sum()),
                neutral_reviews=('sentiment_score', lambda x: (x == 1).sum()),
                positive_reviews=('sentiment_score', lambda x: (x == 2).sum()),
                ).reset_index()

                # Фильтруем только те АЗС, где общее количество отзывов больше или равно 20
                review_counts = review_counts[review_counts['total_reviews'] >= 20]

                # Вычисляем процент отзывов для каждой АЗС
                review_counts['negative_ratio'] = review_counts['negative_reviews'] / review_counts['total_reviews'] * 100
                review_counts['neutral_ratio'] = review_counts['neutral_reviews'] / review_counts['total_reviews'] * 100
                review_counts['positive_ratio'] = review_counts['positive_reviews'] / review_counts['total_reviews'] * 100

            except:
                pass

            try:
                st.subheader("Список АЗС с наибольшим количеством негативных отзывов")
                # Сортируем по проценту негативных отзывов в порядке убывания
                top_negative = review_counts.sort_values(by='negative_ratio', ascending=False).head(20).reset_index()
                
                fig, ax = plt.subplots(figsize=(10, 6))
                sns.barplot(data=top_negative, x='object_id', y='negative_ratio', order=top_negative['object_id'])
                plt.title("Топ-20 АЗС с наибольшим процентом негативных отзывов")
                plt.xlabel("АЗС ID")
                plt.xticks(rotation=90)
                plt.ylabel("Процент негативных отзывов")
                st.pyplot(plt)
                st.session_state.figures.append(fig)
                st.text(top_negative)
            except: pass


            try:
                # Получаем отзывы с негативным sentiment_score
                exploded_df = st.session_state.analyzed_reviews_df.explode('keywords')
                negative_reviews = exploded_df[exploded_df['sentiment_score'] == 0]

                # Группируем по 'object_id' и 'keywords', чтобы посчитать количество негативных отзывов по категориям
                category_counts = negative_reviews.groupby(['object_id', 'keywords']).size().unstack().fillna(0)

                # Фильтруем category_counts по top_negative, чтобы оставить только те АЗС, которые есть в top_negative
                filtered_category_counts = category_counts.loc[category_counts.index.isin(top_negative['object_id'])]

                # Строим тепловую карту
                fig, ax = plt.subplots(figsize=(12, 8))
                sns.heatmap(filtered_category_counts, annot=True, cmap='Reds', fmt='g', linewidths=0.5)
                plt.title("Тепловая карта: Негативные отзывы по категориям для топ-20 АЗС")
                plt.xlabel("Категория")
                plt.ylabel("АЗС ID")
                plt.xticks(rotation=90)
                plt.tight_layout()
                st.pyplot(plt)
                st.session_state.figures.append(fig)

            except: 
                pass

            try:
                # Построение графика
                fig, ax = plt.subplots(figsize=(12, 8))
                
                hourly_sentiment_counts = st.session_state.analyzed_reviews_df.groupby(['hour', 'sentiment_score']).size().unstack(fill_value=0)

                plt.figure(figsize=(10, 6))
                # Настройки графика
                plt.title('Распределение отзывов по времени суток по тональности')
                plt.xlabel('Час')
                plt.ylabel('Количество отзывов')
                plt.xticks(range(0, 24, 1))
                plt.legend(title='Тип отзыва')
                plt.tight_layout()
                # Строим линии для разных тональностей
                sns.lineplot(x=hourly_sentiment_counts.index, y=hourly_sentiment_counts[2], marker='o', label='Положительные', color='green')
                sns.lineplot(x=hourly_sentiment_counts.index, y=hourly_sentiment_counts[1], marker='o', label='Нейтральные', color='blue')
                sns.lineplot(x=hourly_sentiment_counts.index, y=hourly_sentiment_counts[0], marker='o', label='Негативные', color='red')
                st.pyplot(plt)
                st.session_state.figures.append(fig)

            except:
                pass

            
            try:
                # Добавляем колонку с месяцем
                st.session_state.analyzed_reviews_df['month'] = st.session_state.analyzed_reviews_df['comment_time'].dt.month

                # Группируем по месяцам и считаем количество отзывов
                monthly_counts = st.session_state.analyzed_reviews_df.groupby('month').size()

                # Построение графика
                fig, ax = plt.subplots(figsize=(12, 8))
                sns.barplot(x=monthly_counts.index, y=monthly_counts.values, palette='viridis')
                plt.title('Количество отзывов по месяцам')
                plt.xlabel('Месяц')
                plt.ylabel('Количество отзывов')
                plt.xticks(range(12), ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'])
                plt.tight_layout()
                st.pyplot(plt)
                st.session_state.figures.append(fig)
            except:
                pass
            
            try:
                # Группируем по времени, ключевому слову и тональности
                keyword_sentiment_hour_counts = (
                    exploded_df.groupby(['hour', 'keywords', 'sentiment_score'])
                    .size()
                    .unstack(fill_value=0)
                )

                # Определяем уникальные ключевые слова
                keywords = keyword_sentiment_hour_counts.index.get_level_values('keywords').unique()

                # Создаем несколько подграфиков (по одному для каждого ключевого слова)
                fig, axes = plt.subplots(len(keywords), 1, figsize=(14, len(keywords) * 5))

                # Если только один подграфик, axes будет не массивом, а объектом, так что сделаем проверку
                if len(keywords) == 1:
                    axes = [axes]

                for i, keyword in enumerate(keywords):
                    # Фильтруем данные по ключевому слову
                    data = keyword_sentiment_hour_counts.xs(keyword, level='keywords')
                    
                    # Строим график для каждого ключевого слова
                    data.plot(kind='bar', stacked=True, ax=axes[i], colormap='coolwarm', width=0.8)
                    
                    axes[i].set_title(f'Тональность отзывов для "{keyword}"', fontsize=14)
                    axes[i].set_xlabel('Час дня', fontsize=12)
                    axes[i].set_ylabel('Частота', fontsize=12)
                    axes[i].legend(title='Тональность', fontsize=10)

                # Выводим график в Streamlit
                st.pyplot(fig)
                st.session_state.figures.append(fig)
            except:
                pass

            try:
                keyword_sentiment = exploded_df.groupby('keywords').agg(
                    avg_sentiment=('sentiment_score', 'mean'),
                    review_count=('sentiment_score', 'count')
                ).reset_index()

                fig, ax = plt.subplots(figsize=(10, 6))
                plt.scatter(keyword_sentiment['keywords'], keyword_sentiment['avg_sentiment'], s=keyword_sentiment['review_count']*0.1, alpha=0.7)
                plt.title('Тональность и количество отзывов по категориям')
                plt.xlabel('Ключевое слово')
                plt.ylabel('Средняя тональность')
                plt.xticks(rotation=90)

                st.pyplot(fig)
                st.session_state.figures.append(fig)

            except:
                pass

        else:
             for fig in st.session_state.figures:
                st.pyplot(fig)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_filename = f"reports\\review_report_{timestamp}.pdf"
        with PdfPages(pdf_filename) as pdf:
            for figure in st.session_state.figures:
                pdf.savefig(figure)  # Сохраняем каждый график
        
        
        with open(pdf_filename, "rb") as file:
            st.download_button(label="Скачать PDF", data=file, file_name=pdf_filename, mime="application/pdf")

if __name__ == "__main__":
    main()
            

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

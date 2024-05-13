import pandas as pd
import string
import re
import nltk
from nltk.corpus import words
from nltk.tokenize import word_tokenize
import wordninja
import time

start_time = time.time()

stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
             "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
             "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
             "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
             "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
             "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
             "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
             "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
             "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
             "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

# nltk'nin ingilizce kelimeler listesi
english_vocab = set(words.words())

# HTML etiketlerini kaldırmak için düzenli ifade
html_pattern = re.compile('<.*?>')


def clean_text(text):

    # HTML etiketlerini kaldırma
    text = re.sub(html_pattern, '', text)

    text_list = wordninja.split(text)
    text_string = ' '.join(text_list)

    # Noktalama işaretlerini kaldırma
    text_string = "".join([char for char in text_string if char not in string.punctuation])

    # Sayıları kaldırma
    text_string = re.sub(r'\d+', '', text_string)
    # Durak kelimelerini (stopwords) kaldırma ve küçük harfe dönüştürme
    text_string = " ".join([word for word in text_string.lower().split() if word not in stopwords])

    # Spell checker
    text_string = " ".join([word if word in english_vocab else word for word in text_string.split()])

    # Tek boşluğa düşürme
    text_string = re.sub(r'\s+', ' ', text_string).strip()

    return text_string


# CSV dosyasını yükleme
df = pd.read_csv('D:/NACSV/rating_not_null.csv')


# Paralel işlemlerden gelen sonuçları toplama
max_review_word = ""
max_title_word = ""
max_review_word_length = 0
max_title_word_length = 0
max_review_length = 0
max_review_word_count = 0

for index, row in df.iterrows():
    # Review ve Title sütunlarını temizleme

    review_text_cleaned = clean_text(row['review'])
    title_text_cleaned = clean_text(row['title'])

    max_review_length = max(max_review_length, len(review_text_cleaned))

    # En uzun kelime uzunluğunu kontrol etme
    review_words = review_text_cleaned.split()
    title_words = title_text_cleaned.split()

    # Eğer title_words boş değilse, en uzun kelime uzunluğunu kontrol et
    if title_words:
        max_title_word_length_curr = max(len(word) for word in title_words)
        if max_title_word_length_curr > max_title_word_length:
            max_title_word_length = max_title_word_length_curr
            max_title_word = max(title_words, key=len)

    # Eğer review_words boş değilse, en uzun kelime uzunluğunu kontrol et
    if review_words:
        max_review_word_length_curr = max(len(word) for word in review_words)
        if max_review_word_length_curr > max_review_word_length:
            max_review_word_length = max_review_word_length_curr
            max_review_word = max(review_words, key=len)

        review_word_count = len(review_words)
        max_review_word_count = max(max_review_word_count, review_word_count)

    print(f"rating: {row['rating']} title: {title_text_cleaned}  review: {review_text_cleaned}")

print(f"En uzun kelime: '{max_review_word}', Uzunluğu: {max_review_word_length}, Review sütununda")
print(f"En uzun kelime: '{max_title_word}', Uzunluğu: {max_title_word_length}, Title sütununda")
print(f"En uzun review'un karakter sayısı: {max_review_length}")
print(f"En uzun review'da {max_review_word_count} kelime bulunmaktadır.")
end_time = time.time()
# Kodun çalışma süresini hesaplama
execution_time = end_time - start_time
print(f"Kodun çalışma süresi: {execution_time} saniye")
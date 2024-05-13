import pandas as pd
import string
import re
import nltk
from nltk.corpus import words
from nltk.tokenize import word_tokenize
import wordninja
import concurrent.futures
import time
import multiprocessing
import csv
start_time = time.time()

stopwords = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
             "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
             "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
             "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do",
             "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
             "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
             "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
             "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
             "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
             "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"}

# nltk'nin ingilizce kelimeler listesi
english_vocab = set(words.words())

# HTML etiketlerini kaldırmak için düzenli ifade
html_pattern = re.compile('<.*?>')


def clean_text(text):
    # HTML etiketlerini kaldırma
    text = re.sub(html_pattern, '', text)
    #Splitting Words
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
def process_chunk(chunk):
    # Paralel işlemlerden gelen sonuçları toplama
    max_review_word = ""
    max_title_word = ""
    max_review_word_length = 0
    max_title_word_length = 0
    max_review_length = 0
    max_review_word_count = 0

    with open('results.csv', 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['rating', 'title', 'review']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        for index, row in chunk.iterrows():
            review_text_cleaned = clean_text(row['review'])
            title_text_cleaned = clean_text(row['title'])

            max_review_length = max(max_review_length, len(review_text_cleaned))

            review_words = review_text_cleaned.split()
            title_words = title_text_cleaned.split()

            if title_words:
                max_title_word_length_curr = max(len(word) for word in title_words)
                if max_title_word_length_curr > max_title_word_length:
                    max_title_word_length = max_title_word_length_curr
                    max_title_word = max(title_words, key=len)

            if review_words:
                max_review_word_length_curr = max(len(word) for word in review_words)
                if max_review_word_length_curr > max_review_word_length:
                    max_review_word_length = max_review_word_length_curr
                    max_review_word = max(review_words, key=len)

                review_word_count = len(review_words)
                max_review_word_count = max(max_review_word_count, review_word_count)

            # Her bir satırı CSV dosyasına yaz
            writer.writerow({'rating': row['rating'], 'title': title_text_cleaned, 'review': review_text_cleaned})

            print(f"rating: {row['rating']} title: {title_text_cleaned}  review: {review_text_cleaned}")

    return max_review_word, max_title_word, max_review_word_length, max_title_word_length, max_review_length, max_review_word_count


if __name__ == '__main__':
    multiprocessing.freeze_support()

    chunksize = 10000 # Her bir parçanın satır sayısı
    chunks = pd.read_csv('final.csv', chunksize=chunksize)
    max_review_words = []
    max_title_words = []
    max_review_word_lengths = []
    max_title_word_lengths = []
    max_review_lengths = []
    max_review_word_counts = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for chunk in chunks:
            futures.append(executor.submit(process_chunk, chunk))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            max_review_words.append(result[0])
            max_title_words.append(result[1])
            max_review_word_lengths.append(result[2])
            max_title_word_lengths.append(result[3])
            max_review_lengths.append(result[4])
            max_review_word_counts.append(result[5])

    # En uzun kelimeleri ve bilgileri bul
    max_review_word = max(max_review_words, key=len)
    max_title_word = max(max_title_words, key=len)
    max_review_word_length = max(max_review_word_lengths)
    max_title_word_length = max(max_title_word_lengths)
    max_review_length = max(max_review_lengths)
    max_review_word_count = max(max_review_word_counts)

    # Sonuçları yazdır
    print(f"En uzun kelime: '{max_review_word}', Uzunluğu: {max_review_word_length}, Review sütununda")
    print(f"En uzun kelime: '{max_title_word}', Uzunluğu: {max_title_word_length}, Title sütununda")
    print(f"En uzun review'un karakter sayısı: {max_review_length}")
    print(f"En uzun review'da {max_review_word_count} kelime bulunmaktadır.")
    end_time = time.time()
    # Kodun çalışma süresini hesaplama
    execution_time = end_time - start_time
    print(f"Kodun çalışma süresi: {execution_time} saniye")

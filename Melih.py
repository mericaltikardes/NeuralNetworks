import pandas as pd
import random

seed_value = 3131
rating_amount =1

# CSV dosyasını oku
df = pd.read_csv('D:/results.csv')

# Karıştırma için seed ayarla
random.seed(seed_value)

# Boş DataFrame oluştur
sampled_df = pd.DataFrame(columns=['rating', 'title', 'review'])

# Belirli bir rating aralığındaki verileri ekleyin
for rating in range(1, 11):
    # Belirli bir rating'e sahip olan satırları seçin
    selected_rows = df[df['rating'] == rating].sample(n=rating_amount, random_state=seed_value)
    # Seçilen satırları sampled DataFrame'ine ekle
    sampled_df = pd.concat([sampled_df, selected_rows])

# Karıştırma
sampled_df_shuffled = sampled_df.sample(frac=1, random_state=seed_value).reset_index(drop=True)

print(sampled_df_shuffled.head(10))  # İlk 10 satırı yazdır

# 'final.csv' dosyasını kaydedin
sampled_df_shuffled.to_csv('final.csv', index=False)

print("CSV dosyası oluşturuldu.")

print(sampled_df_shuffled['rating'].value_counts())

print("Toplam satır sayısı:", sampled_df_shuffled.shape[0])

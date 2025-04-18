from tensorflow.keras.models import load_model
import pickle
from tensorflow.keras.preprocessing.sequence import pad_sequences
import numpy as np

# Загружаем модель
model = load_model("AI/sentiment_model.keras")

# Загружаем токенизатор
with open("AI/tokenizer.pkl", "rb") as f:
    tokenizer = pickle.load(f)

def predict_sentiment(text):
    if not isinstance(text, str) or not text.strip():
        return "Нетральный"  # или np.nan
    sequence = tokenizer.texts_to_sequences([text])
    padded = pad_sequences(sequence, maxlen=100)
    prediction = model.predict(padded)[0]
    class_id = np.argmax(prediction)
    result = ['Нетральный', "Негативный", "Позитивный"]
    return result[class_id]

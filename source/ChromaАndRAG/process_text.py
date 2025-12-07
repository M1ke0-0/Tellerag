import os
import nltk
import emoji
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

full_path = os.path.dirname(os.path.abspath(__file__))


def preprocess_text(text: str, lang: str = "russian") -> str:
    nltk.download('punkt_tab', download_dir='./../../nltk_data')
    nltk.download('stopwords', download_dir='./../../nltk_data')
    nltk.data.path.append(f'{full_path}/../../nltk_data')
    text = emoji.replace_emoji(text, replace='')
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    tokens = word_tokenize(text, language=lang)
    stop_words = set(stopwords.words(lang))
    tokens = [word for word in tokens if word not in stop_words]
    return ' '.join(tokens)

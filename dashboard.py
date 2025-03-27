import streamlit as st
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["yelp"]
collection = db["reviews"]

# Streamlit Page Config
st.set_page_config(page_title="Real-Time Yelp Review Dashboard", layout="wide")

st.title("📈 Real-Time Yelp Review Analyzer")

# Load reviews from MongoDB
reviews = list(collection.find().sort("timestamp", -1).limit(200))
if not reviews:
    st.warning("No data found in MongoDB. Make sure Spark Streaming is running.")
    st.stop()

# Convert to DataFrame
df = pd.DataFrame(reviews)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# ====================
# 📊 Sentiment Pie Chart
# ====================
st.subheader("Sentiment Breakdown")
sentiment_counts = df['sentiment'].value_counts()
fig1, ax1 = plt.subplots()
ax1.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%', startangle=90)
ax1.axis('equal')
st.pyplot(fig1)

# ====================
# ⏳ Sentiment Over Time
# ====================
st.subheader("Sentiment Over Time")
df_time = df.set_index('timestamp').resample('1Min').sentiment.value_counts().unstack().fillna(0)
st.line_chart(df_time)

# ====================
# 📜 Latest Reviews Feed
# ====================
st.subheader("Latest Reviews")
for _, row in df.head(10).iterrows():
    st.markdown(f"""
    **Sentiment:** {row['sentiment'].capitalize()}  
    **Stars:** {row['stars']}  
    **Review:** {row['text']}  
    **Timestamp:** {row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}
    ---
    """)

# ====================
# 🔍 (Optional) Fake Review Detector
# ====================
# Example rule: very short + 5 stars = suspicious
st.subheader("🚨 Suspicious Reviews")
df['length'] = df['text'].apply(lambda x: len(x.strip()))
suspicious = df[(df['stars'] == 5) & (df['length'] < 40)]

if suspicious.empty:
    st.success("No suspicious reviews found.")
else:
    for _, row in suspicious.iterrows():
        st.markdown(f"""
        ⚠️ **Suspicious 5⭐ Review**  
        **Review ID:** {row['review_id']}  
        **Text:** {row['text']}  
        **Length:** {row['length']} characters  
        **Timestamp:** {row['timestamp']}
        ---
        """)

